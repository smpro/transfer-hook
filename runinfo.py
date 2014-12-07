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
    logging.basicConfig(level = _logging_level)
    setup()
    run_numbers.extend(sys.argv[1:])
    run_numbers.sort()
    results = get_hlt_key(run_numbers)
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
def get_cmssw_version(run_numbers):
    return get_parameter('CMS.DAQ:DAQ_CMSSW_VERSION_T', run_numbers)
## get_cmssw_version


#______________________________________________________________________________
def get_run_key(run_numbers):
    return get_parameter('CMS.DAQ:DAQ_RUN_KEY', run_numbers)
## get_run_key


#______________________________________________________________________________
def get_hlt_key(run_numbers):
    return get_parameter('CMS.LVL0:HLT_KEY_DESCRIPTION', run_numbers)
## get_hlt_key


#______________________________________________________________________________
def get_parameter(name, run_numbers):
    cursor = connection.cursor()
    query = """
        SELECT STRING_VALUE
        FROM CMS_RUNINFO.RUNSESSION_PARAMETER
        WHERE RUNNUMBER=:run_number and
        NAME='%s'
        """ % name
    logger.debug("Using SQL query: `%s'" % query)
    cursor.prepare(query)
    result = []
    try:
        for run_number in run_numbers:
            result.append(execute_query(cursor, run_number))            
    except TypeError:
        run_number = run_numbers
        result = execute_query(cursor, run_number)
    return result
## get_parameter


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

