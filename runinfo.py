#!/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys

import cx_Oracle

logger = logging.getLogger(__name__)

_logging_level = logging.WARNING
_db_config = '.db.omds.runinfo_r.cfg.py'

run_numbers = [
    229221,
    ]

execfile(_db_config)
_sid    = sid
_reader = reader
_phrase = phrase


#______________________________________________________________________________
def main():
    global run_numbers
    logging.basicConfig(level = _logging_level)
    setup()
    run_numbers.extend(sys.argv[1:])
    run_numbers = map(int, run_numbers)
    run_numbers.sort()
    # results = get_hlt_key(run_numbers)
    results = get_cmssw_versions(run_numbers)
    for run_number, result in zip(run_numbers, results):
        print run_number, result
    logger.info('End')
    teardown()
## main


#______________________________________________________________________________
def setup():
    global connection
    connection = cx_Oracle.connect(_reader, _phrase, _sid)
## setup

    
#______________________________________________________________________________
def get_cmssw_version(run_number):
    return get_parameter('CMS.DAQ:DAQ_CMSSW_VERSION_T', run_number)
## get_cmssw_version


#______________________________________________________________________________
def get_run_key(run_number):
    return get_parameter('CMS.DAQ:DAQ_RUN_KEY', run_number)
## get_run_key


#______________________________________________________________________________
def get_hlt_key(run_number):
    return get_parameter('CMS.LVL0:HLT_KEY_DESCRIPTION', run_number)
## get_hlt_key


#______________________________________________________________________________
def get_cmssw_versions(run_numbers):
    return get_parameters('CMS.DAQ:DAQ_CMSSW_VERSION_T', run_numbers)
## get_cmssw_versions


#______________________________________________________________________________
def get_run_keys(run_numbers):
    return get_parameters('CMS.DAQ:DAQ_RUN_KEY', run_numbers)
## get_run_keys


#______________________________________________________________________________
def get_hlt_keys(run_numbers):
    return get_parameters('CMS.LVL0:HLT_KEY_DESCRIPTION', run_numbers)
## get_hlt_keys


#______________________________________________________________________________
def get_parameter(name, run_number):
    cursor = connection.cursor()
    prepare_cursor(cursor, name)
    return execute_query(cursor, run_number)
## get_parameter


#______________________________________________________________________________
def get_parameters(name, run_numbers):
    cursor = connection.cursor()
    prepare_cursor(cursor, name)
    results = []
    for run_number in run_numbers:
        results.append(execute_query(cursor, run_number))
    return results
## get_parameter


#______________________________________________________________________________
def prepare_cursor(cursor, name):
    query = """
        SELECT STRING_VALUE
        FROM CMS_RUNINFO.RUNSESSION_PARAMETER
        WHERE RUNNUMBER=:run_number and
        NAME='%s'
        """ % name
    logger.debug("Using SQL query: `%s'" % query)
    cursor.prepare(query)
## prepare


#______________________________________________________________________________
def execute_query(cursor, run_number):
    logger.debug('Executing query for run %d ...' % run_number)
    cursor.execute(None, {'run_number': run_number})
    try:
        result = cursor.next()[0]
    except StopIteration:
        result = 'UNKNOWN'
    result = result.strip()
    logger.debug("Received `{0}'".format(result))
    return result
## execute_query


#______________________________________________________________________________
def teardown():
    connection.close()
## teardown

    
#______________________________________________________________________________
if __name__ == '__main__':
    import user
    main()

