# -*- coding: utf-8 -*-
import cx_Oracle
import sys

_verbosity = 10
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
    run_numbers.extend(sys.argv[1:])
    run_numbers.sort()
    cmssw_versions = get_cmssw_version_for_runs(run_numbers)
    for run_number, result in zip(run_numbers, cmssw_versions):
        print run_number, result
    print 'End'
## main


#______________________________________________________________________________
def get_cmssw_version_for_runs(run_numbers):
    connection = cx_Oracle.connect(_reader, _phrase, _sid)
    cursor = connection.cursor()
    query = """
        SELECT STRING_VALUE
        FROM CMS_RUNINFO.RUNSESSION_PARAMETER
        WHERE RUNNUMBER=:run_number and
        NAME='CMS.DAQ:DAQ_CMSSW_VERSION_T'
        """
    cursor.prepare(query)
    results = []
    for run_number in run_numbers:
        print 'Checking run', run_number
        cursor.execute(None, {'run_number': run_number})
        try:
            result = cursor.next()[0]
        except StopIteration:
            result = 'UNKNONW'
        results.append(result.strip())
    connection.close()
    return results
## get_cmssw_version_for_runs


#______________________________________________________________________________
if __name__ == '__main__':
    import user
    main()

