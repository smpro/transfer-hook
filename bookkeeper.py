#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Extracts the list of streams and lumis for a given run and the number of
files for a given lumi of a given run and stream.

Jan Veverka, 3 September 2014, veverka@mit.edu

TODO: 
  * Check the meaning of CMS_STOMGR.runs.status from the twiki 
    (done 2014/09/08)
  * Understand the Tier0-usage query (done 2014/09/08)
  * Use the *_len shortcut variables for the string lengths in single json 
    parsing
  * Use SQL variable binding 
  * Use SQL prepared statements
'''
import os
import json
import glob
import pprint
import socket
import cx_Oracle

#run 225*   files
#----------------
#075        37
#080        91
#115        15
#117        213
#119        200
#125        844
#----------------
#total      1400

# _db_config = '.db.int2r.stomgr_w.cfg.py'
_db_config = '.db.rcms.stomgr_w.cfg.py'
execfile(_db_config)
_db_sid = db_sid
_db_user = db_user
_db_pwd = db_pwd
_input_dir = '/store/lustre/mergeMacro'
_run_number = 226490
## EventDisplay and DQMHistograms should not be transferred
## DQM should be transferred but it's taken out because it causes 
## problems
_excluded_streams = ['EventDisplay', 'DQMHistograms', 'DQM']

#_______________________________________________________________________________
def main():
    run_dir            = os.path.join(_input_dir, 'run%d' % _run_number)
    json_filenames     = get_json_filenames(run_dir)
    stream_lumi_map    = parse_json_filenames(json_filenames)
    last_lumi          = get_last_lumi(stream_lumi_map)
    ## Dictionary of lists giving the number of files for given lumi and stream.
    ## Stream is given by the dictionary key. The dictionary value is a list
    ## of the file counts with one entry per each lumi section. The lumi section
    ## is given by the index of the list item increased by 1:
    ## s -> [files_per_lumi_1, files_per_lumi_2, ..., files_per_lumi_N]
    ## Here, files_per_lumi_i is the number of files in the lumi i of
    ## the stream S. The index of this element is j = i - 1, so that i = j + 1.
    ## N is the total number of lumis, which is equal to the number of the
    ## last lumi stored in the last_lumi variable
    files_per_lumi = {}
    for stream, lumi_map in stream_lumi_map.items():
        if stream in _excluded_streams:
            print 'Skipping stream', stream
            continue
        files_per_lumi[stream] = get_files_per_lumi(lumi_map, last_lumi)
    # report(stream_lumi_map, files_per_lumi)
    connection = cx_Oracle.connect(_db_user, _db_pwd, _db_sid)
    cursor = connection.cursor()
    fill_streams(files_per_lumi, cursor)
    fill_runs(last_lumi, cursor)
    connection.commit()
    connection.close()
## main


#_______________________________________________________________________________
def get_json_filenames(run_dir):
    mask = '*.jsn'
    pathname = os.path.join(run_dir, mask)
    json_filenames = glob.glob(pathname)
    json_filenames = [os.path.basename(f) for f in json_filenames]
    return json_filenames
## get_json_filenames


#_______________________________________________________________________________
def parse_json_filenames(json_filenames):
    stream_lumi_map = {}
    streams = set()
    last_lumi = 0
    for json in json_filenames:
        meta_data = parse_single_json_filename(json)
        if meta_data:
            run, lumi, stream = meta_data
        else:
            continue
        if stream in stream_lumi_map:
            stream_lumi_map[stream][lumi] = json
        else:
            stream_lumi_map[stream] = {lumi: json}
    return stream_lumi_map
## parse_json_filenames


#_______________________________________________________________________________
def get_last_lumi(stream_lumi_map):
    last_lumi = 0
    for lumi_map in stream_lumi_map.values():
        last_lumi = max(last_lumi, max(lumi_map.keys()))
    return last_lumi
## get_last_lumi


#_______________________________________________________________________________
def parse_single_json_filename(filename):
    '''Extracts the run number, stream name and luminosity section number
    from the given filename of the json file.
    '''
    ## A JSON filename looks for example like this:
    ## run225115_ls0011_streamALCAPHISYM_StorageManager.jsn
    root, extention = os.path.splitext(filename)
    tokens = root.split('_')
    skip_message = "INFO: Skipping `%s' ..." % filename
    if len(tokens) != 4:
        print skip_message
        return None
    run_token, ls_token, stream_token, sm_token = tokens
    run_len, ls_len, stream_len, sm_len = map(len, tokens)
    if (run_token   [:len('run')   ] != 'run'         or
        ls_token    [:len('ls')    ] != 'ls'          or
        stream_token[:len('stream')] != 'stream'      or
        sm_token                     != 'StorageManager'):
        print skip_message
        return None

    run    = int(run_token   [len('run')   :])
    lumi   = int(ls_token    [len('ls')    :])
    stream =     stream_token[len('stream'):]
    return run, lumi, stream
## parse_single_json_filename


#_______________________________________________________________________________
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


#_______________________________________________________________________________
def get_number_of_files(filename):
    '''Returns the number of files for the given run, stream and luminosity
    section.  This is either 0 or 1. It is 0 if no data file is present or
    if there is no accepted events in the data file.'''
    run_dir   = os.path.join(_input_dir, 'run%d' % _run_number)
    full_path = os.path.join(run_dir, filename)
    if not os.path.exists(full_path):
        return 0
    print 'Parsing', full_path
    with open(full_path) as source:
        meta_data = json.load(source)
    number_of_accepted_events = meta_data['data'][1]
    if number_of_accepted_events == 0:
        return 0
    else:
        return 1
## get_number_of_files


#_______________________________________________________________________________
def get_json_filename(stream, lumi):
    '''Returns the filename string for the JSON file corresponding to the
    given run, stream and lumi.'''
    ## Eample: The call get_json_filename(225115, 11, 'ALCAPHISYM') returns:
    ## 'run225115_ls0011_streamALCAPHISYM_StorageManager.jsn'
    return 'run%06d_ls%04d_stream%s_StorageManager.jsn' % (_run_number, lumi,
                                                           stream)
## get_json_filename

#_______________________________________________________________________________
def report(stream_lumi_map, files_per_lumi):
    pprint.pprint(stream_lumi_map.keys())
    pprint.pprint(files_per_lumi)
## report


#_______________________________________________________________________________
def fill_streams(files_per_lumi, cursor):
    for stream, records in files_per_lumi.items():
        for record in records:
            fill_number_of_files(cursor, stream, **record)
## fill_streams


#_______________________________________________________________________________
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
        ctime       = "TO_DATE('2014-09-22 18:22:07', 'YYYY-MM-DD HH24:MI:SS')",
        eols        = 1,
        )
    insert(values_to_insert, target_table, cursor)
## fill_number_of_files


#_______________________________________________________________________________
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
        start_time       = "TO_DATE('2014-09-22 18:22:07', 'YYYY-MM-DD HH24:MI:SS')",
        ## dummy for now
        end_time         = "TO_DATE('2014-09-22 18:22:07', 'YYYY-MM-DD HH24:MI:SS')", 
        max_lumisection  = last_lumi,
        last_consecutive = last_lumi,
        )
    insert(values_to_insert, target_table, cursor)
## fill_runs


#_______________________________________________________________________________
def insert(values, table, cursor):
    columns = values.keys()
    columns_in_curly_brackets = ['{%s}' % c for c in columns]
    line_to_format = '    (%s)' % ', '.join(columns_in_curly_brackets)
    lines = ['insert into %(table)s' % locals(),
             '    (%s)' % ', '.join(columns)      ,
             'values'                            ,
             line_to_format.format(**values)     ]
    statement = '\n'.join(lines)
    print 'SQL>', statement, ';'
    cursor.execute(statement)
## insert
    

#_______________________________________________________________________________
if __name__ == '__main__':
    main()
    import user

