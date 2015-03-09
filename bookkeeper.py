#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Extracts the list of streams and luminosity sections (lumis) for a given
run and the number of files for a given lumi of a given run and stream
and inserts these in the database.  Supports also insertion of data only
for lumis with missing files to fill in gaps of consecutive luminosity
sections.

Jan Veverka, 3 September 2014, veverka@mit.edu
Last update: 11 December 2014

TODO:
  * Use the logging module for logging
  * Notify Tier0 about the open runs, insert -> modify
  * Puppet-ize
  * pip-ify
  * Turn the main functions into a class
  * Use SQL variable binding
  * Use SQL prepared statements
  * Use actual time stamps
  * Avoid logging a warning for zero missing lumis
  * Load db config in a separate method
'''

__author__     = 'Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = ['Hannes Sakulin', 'Dirk Hufnagel', 'Lavinia Darlea',
                  'Guillelmo Gomez-Ceballos', 'Remi Mommsen']
__licence__    = 'Unknonw'
__version__    = '0.1.3'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'

import glob
import imp
import json
import logging
import os.path
import pprint
import socket
import sys

import cx_Oracle

import transfer.hook.config as config

from collections import defaultdict

logger = logging.getLogger(__name__)

_dry_run = False
# Integration DB, will not be read by Tier0
#_db_config = '.db.int2r.stomgr_w.cfg.py'

# Production DB, will be read and processed by Tier0
_db_config = os.path.join(config.DIR, '.db_rcms_cred.py')


# _input_dir = '/store/lustre/mergeMacro'
_input_dir = '/store/lustre/transfer'
#_input_dir = '/store/lustre/transfer_minidaq'
_run_number = 229781
## List of streams that should be ignored by the Tier0
_excluded_streams = ['EventDisplay', 'DQMHistograms', 'DQM', 'DQMCalibration',
                     'CalibrationDQM', 'Error', 'HLTRates', 'L1Rates']

#______________________________________________________________________________
def main():
    logger.info('*** Closing run %d ... ***' %  _run_number)
    setup()
    run_dir            = os.path.join(_input_dir, 'run%d' % _run_number)
    json_filenames     = get_json_filenames(run_dir)
    stream_lumi_map    = parse_json_filenames(json_filenames)
    last_lumi          = get_last_lumi(stream_lumi_map)
    ## Dictionary of lists giving the number of files for given lumi and
    ## stream.
    ## Stream is given by the dictionary key. The dictionary value is a list
    ## of the file counts with one entry per each lumi section. The lumi
    ## section
    ## is given by the index of the list item increased by 1:
    ## s -> [files_per_lumi_1, files_per_lumi_2, ..., files_per_lumi_N]
    ## Here, files_per_lumi_i is the number of files in the lumi i of
    ## the stream S. The index of this element is j = i - 1, so that
    ## i = j + 1.
    ## N is the total number of lumis, which is equal to the number of the
    ## last lumi stored in the last_lumi variable
    files_per_lumi = {}
    missing_lumi_map = {}
    present_lumi_map = {}
    for stream, lumi_map in stream_lumi_map.items():
        if stream in _excluded_streams:
            logger.debug('Skipping stream %s.' % stream)
            continue
        files_per_lumi[stream] = get_files_per_lumi(lumi_map, last_lumi)
        present_lumis = lumi_map.keys()
        missing_lumis = get_missing_lumis(present_lumis, last_lumi)
        if missing_lumis:
            logger.warning(
                'Missing lumis for run %d, stream %s: ' % (
                    _run_number, stream
                ) + str(missing_lumis)
            )
        present_lumi_map[stream] = present_lumis
        missing_lumi_map[stream] = missing_lumis
    logger.debug('Connecting to %s as %s' % (_db_sid, _db_user))
    connection = cx_Oracle.connect(_db_user, _db_pwd, _db_sid)
    cursor = connection.cursor()
    #fill_streams(files_per_lumi, cursor, lumis_to_skip=missing_lumi_map)
    fill_missing_lumis(missing_lumi_map, cursor)
    #fill_runs(last_lumi, cursor)
    close_run(last_lumi, cursor)
    connection.commit()
    connection.close()
    logger.info('Closed run %d.' %  _run_number)
## main


#______________________________________________________________________________
def setup():
    global execute_sql
    global _db_sid, _db_user, _db_pwd
    if _dry_run:
        logger.info('Setting up a dry run.')
        execute_sql = lambda cursor, statement: None ## Does nothing
    else:
        execute_sql = lambda cursor, statement: cursor.execute(statement)
    db_config = imp.load_source('db_config', _db_config)
    _db_sid = db_config.db_sid
    _db_user = db_config.db_user
    _db_pwd = db_config.db_pwd
## setup


#______________________________________________________________________________
def get_json_filenames(run_dir):
    mask = '*.jsn'
    pathname = os.path.join(run_dir, mask)
    json_filenames = glob.glob(pathname)
    json_filenames = [os.path.basename(f) for f in json_filenames]
    return json_filenames
## get_json_filenames


#______________________________________________________________________________
def parse_json_filenames(json_filenames):
    stream_lumi_map = defaultdict(dict)
    for json in json_filenames:
        try:
            run, lumi, stream = parse_single_json_filename(json)
        except TypeError:
            continue ## not a meta-data JSON file
        stream_lumi_map[stream][lumi] = json
    return stream_lumi_map
## parse_json_filenames


#______________________________________________________________________________
def get_last_lumi(stream_lumi_map):
    last_lumi = 0
    for lumi_map in stream_lumi_map.values():
        last_lumi = max(last_lumi, max(lumi_map.keys()))
    return last_lumi
## get_last_lumi


#______________________________________________________________________________
def parse_single_json_filename(filename):
    '''Extracts the run number, stream name and luminosity section number
    from the given filename of the json file.
    '''
    ## A JSON filename looks for example like this:
    ## run225115_ls0011_streamALCAPHISYM_StorageManager.jsn
    root, extention = os.path.splitext(filename)
    tokens = root.split('_')
    skip_message = "Skipping `%s' ..." % filename
    if len(tokens) != 4:
        logger.debug(skip_message)
        return None
    run_token, ls_token, stream_token, sm_token = tokens
    run_len, ls_len, stream_len, sm_len = map(len, tokens)
    if (run_token   [:len('run')   ] != 'run'         or
        ls_token    [:len('ls')    ] != 'ls'          or
        stream_token[:len('stream')] != 'stream'      or
        sm_token                     != 'StorageManager'):
        logger.debug(skip_message)
        return None

    run    = int(run_token   [len('run')   :])
    lumi   = int(ls_token    [len('ls')    :])
    stream =     stream_token[len('stream'):]
    return run, lumi, stream
## parse_single_json_filename


#______________________________________________________________________________
def get_files_per_lumi(lumi_map, last_lumi):
    records = []
    for lumi in range(1, last_lumi + 1):
        if lumi in lumi_map:
            filename = lumi_map[lumi]
            number_of_files = get_number_of_files(filename)
        else:
            number_of_files = 0
        record = dict(lumi            = lumi,
                      number_of_files = number_of_files)
        records.append(record)
    return records
## get_files_per_lumi


#______________________________________________________________________________
def get_missing_lumis(present_lumis, last_lumi):
    '''
    Given the list of lumis, for which there is a JSON file present, 
    present_lumis, and the last_lumi, returns the list of lumis
    that have a missing JSON file.
    '''
    missing_lumis = []
    for lumi in range(1, last_lumi + 1):
        if lumi not in present_lumis:
            missing_lumis.append(lumi)
    return missing_lumis
## get_missing_lumis


#______________________________________________________________________________
def get_number_of_files(filename):
    '''
    Returns the number of files for the given run, stream and luminosity
    section.  This is either 0 or 1. It is 0 if no data file is present or
    if there is no accepted events in the data file.
    '''
    run_dir   = os.path.join(_input_dir, 'run%d' % _run_number)
    full_path = os.path.join(run_dir, filename)
    if not os.path.exists(full_path):
        return 0
    logger.debug("Parsing `%s' ..." % full_path)
    with open(full_path) as source:
        meta_data = json.load(source)
    number_of_accepted_events = meta_data['data'][1]
    if number_of_accepted_events == 0:
        return 0
    else:
        return 1
## get_number_of_files


#______________________________________________________________________________
def get_json_filename(stream, lumi):
    '''Returns the filename string for the JSON file corresponding to the
    given run, stream and lumi.'''
    ## Eample: The call get_json_filename(225115, 11, 'ALCAPHISYM') returns:
    ## 'run225115_ls0011_streamALCAPHISYM_StorageManager.jsn'
    return 'run%06d_ls%04d_stream%s_StorageManager.jsn' % (_run_number, lumi,
                                                           stream)
## get_json_filename


#______________________________________________________________________________
def fill_streams(files_per_lumi, cursor, lumis_to_skip=defaultdict(list)):
    for stream, records in files_per_lumi.items():
        for record in records:
            if record['lumi'] in lumis_to_skip[stream]:
                continue
            fill_number_of_files(cursor, stream, **record)
## fill_streams


#______________________________________________________________________________
def fill_missing_lumis(missing_lumi_map, cursor):
    for stream, missing_lumis in missing_lumi_map.items():
        for lumi in missing_lumis:
            try:
                fill_number_of_files(cursor, stream, lumi, number_of_files=0)
            except cx_Oracle.IntegrityError:
                logger.warning(
                    ('Failed to insert lumi {0} of run {1}, ' +
                     'stream {2}.').format(lumi, _run_number, stream)
                )
## fill_missing_lumis


#______________________________________________________________________________
def filtered_lumis(lumis_to_keep, lumi_file_map):
    filtered_map = {}
    for lumi, fcount in lumi_file_map.items():
        if lumi in lumis_to_keep:
            filtered_map[lumi] = fcount
    return filtered_map
## filtered_lumis()

#______________________________________________________________________________
def fill_number_of_files(cursor, stream, lumi, number_of_files):
    '''
    Define the values to be filled for the streams table. 
    '''
    target_table = 'cms_stomgr.streams'
    values_to_insert = dict(
        runnumber   = _run_number,
        lumisection = lumi,
        stream      = "'" + stream + "'",
        instance    = 1,
        filecount   = number_of_files,
        ## dummy for now
        ctime       = ("TO_DATE('2014-10-08 14:33:48', " +
                               "'YYYY-MM-DD HH24:MI:SS')"),
        eols        = 1,
        )
    insert(values_to_insert, target_table, cursor)
## fill_number_of_files


#______________________________________________________________________________
def fill_runs(last_lumi, cursor):
    target_table = 'cms_stomgr.runs'
    values_to_insert = dict(
        runnumber        = _run_number,
        instance         = 1,
        hostname         = "'%s'" % socket.gethostname(),
        n_instances      = 1,
        n_lumisections   = last_lumi,
        status           = 0,
        ## dummy for now
        start_time       = ("TO_DATE('2014-10-08 14:33:48', " +
                               "'YYYY-MM-DD HH24:MI:SS')"),
        ## dummy for now
        end_time         = ("TO_DATE('2014-10-08 14:33:48', " +
                               "'YYYY-MM-DD HH24:MI:SS')"),
        max_lumisection  = last_lumi,
        last_consecutive = last_lumi,
        )
    insert(values_to_insert, target_table, cursor)
## fill_runs


#______________________________________________________________________________
def close_run(last_lumi, cursor):
    logger.debug('Doing bookkeeping for run %d ... ' % _run_number)
    try:
        # This should fail
        logger.debug('Trying to insert a record into runs ...')
        fill_runs(last_lumi, cursor)
        logger.debug('Insterted a record into runs.')
        logger.warning('Run %d was not opened!' % _run_number)
    except cx_Oracle.IntegrityError:
        logger.debug('Trying to update the record in runs ...')
        update_run(last_lumi, cursor)
        logger.debug('Updated the record in runs.')
## close_run)


#______________________________________________________________________________
def update_run(last_lumi, cursor):
    target_table = 'cms_stomgr.runs'
    values_to_set = dict(
        n_lumisections   = last_lumi,
        status           = 0,
        ## dummy for now
        end_time         = ("TO_DATE('2014-10-08 14:33:48', " +
                               "'YYYY-MM-DD HH24:MI:SS')"),
        max_lumisection  = last_lumi,
        last_consecutive = last_lumi,
        )
    update(values_to_set, target_table, cursor)
## close_run


#______________________________________________________________________________
def open_run(cursor):
    target_table = 'cms_stomgr.runs'
    values_to_insert = dict(
        runnumber        = _run_number,
        instance         = 1,
        hostname         = "'%s'" % socket.gethostname(),
        n_instances      = 1,
        n_lumisections   = 0,
        status           = 1,
        ## dummy for now
        start_time       = ("TO_DATE('2014-10-08 14:33:48', " +
                               "'YYYY-MM-DD HH24:MI:SS')"),
        ## dummy for now
        end_time         = ("TO_DATE('2014-10-08 14:33:48', " +
                               "'YYYY-MM-DD HH24:MI:SS')"),
        max_lumisection  = 0,
        last_consecutive = 0,
        )
    insert(values_to_insert, target_table, cursor)
## open_run


#______________________________________________________________________________
def insert(values, table, cursor):
    columns = values.keys()
    columns_in_curly_brackets = ['{%s}' % c for c in columns]
    line_to_format = '    (%s)' % ', '.join(columns_in_curly_brackets)
    lines = ['insert into %(table)s' % locals()  ,
             '    (%s)' % ', '.join(columns)     ,
             'values'                            ,
             line_to_format.format(**values)     ]
    for line in lines + [';']:
        logger.debug('SQL> ' + line)
    statement = '\n'.join(lines)
    execute_sql(cursor, statement)
## insert


#______________________________________________________________________________
def update(values, table, cursor):
    columns = values.keys()
    expression_to_format = ', '.join(['%s={%s}' % (c, c) for c in columns])
    lines = ['update %(table)s' % locals()                 ,
             'set ' + expression_to_format.format(**values),
             'where runnumber = %d' % _run_number          ]
    for line in lines + [';']:
        logger.debug('SQL> ' + line)
    statement = '\n'.join(lines)
    execute_sql(cursor, statement)
## insert


#______________________________________________________________________________
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s in %(module)s: %(message)s')
    logger.info("Running `%s' ..." % ' '.join(sys.argv))
    if len(sys.argv) > 1:
        for run_number_as_str in sys.argv[1:]:
            _run_number = int(run_number_as_str)
            main()
    else:
        main()
    import user

