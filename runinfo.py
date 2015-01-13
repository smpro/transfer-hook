#!/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys

import cx_Oracle

import transfer.hook.config as config

logger = logging.getLogger(__name__)

## Default configuration
_defaults = config.Config(
    logging_level = logging.WARNING,
    db_config_file = '.db.omds.runinfo_r.cfg.py',
    run_numbers = [
        229221,
    ]
)

_first_run_with_new_cmssw_version_name_tag = 231017

#______________________________________________________________________________
def main():
    logging.basicConfig(level = _defaults.logging_level)
    logger = logging.getLogger(__name__)
    _defaults.run_numbers += map(int, sys.argv[1:])
    _defaults.run_numbers.sort()
    runinfo = RunInfo(_defaults)
    runinfo.main()
    logger.info('End')
## main


#______________________________________________________________________________
class RunInfo(object):
    def __init__(self, cfg):
        self.logger = logging.getLogger(type(self).__module__ + '.' +
                                        type(self).__name__)
        self.cfg = cfg
        self._setup()
    def _setup(self):
        if hasattr(self.cfg, 'run_numbers'):
            self.run_numbers = self.cfg.run_numbers
        else:
            self.run_numbers = []
        db = config.load(self.cfg.db_config_file)
        self.logger.info('Connecting to %s@%s ...' % (db.reader, db.sid))
        self.connection = cx_Oracle.connect(db.reader, db.phrase, db.sid)
        self.cursor = self.connection.cursor()
    def main(self):
        #dump_runs_with_new_cmssw_tag_name(connection.cursor())
        #dump_cmssw_versions()
        #results = get_hlt_key(run_numbers)
        results = self.get_cmssw_versions(self.run_numbers)
        for run_number, result in zip(self.run_numbers, results):
            print run_number, result
        self.connection.close()
    def get_cmssw_version(self, run_number):
        if run_number < _first_run_with_new_cmssw_version_name_tag:
            tag_name = 'CMS.DAQ:DAQ_CMSSW_VERSION_T'
        else:
            tag_name = 'CMS.DAQ:CMSSW_VERSION'
        return self.get_parameter(tag_name, run_number)
    def get_hlt_key(self, run_number):
        return self.get_parameter('CMS.LVL0:HLT_KEY_DESCRIPTION', run_number)
    def get_run_key(self, run_number):
        return self.get_parameter('CMS.DAQ:DAQ_RUN_KEY', run_number)
    def get_cmssw_versions(self, run_numbers):
        old, new = [], []
        for r in run_numbers:
            if r < _first_run_with_new_cmssw_version_name_tag:
                old.append(r)
            else:
                new.append(r)
        return (
                    self.get_parameters('CMS.DAQ:DAQ_CMSSW_VERSION_T', old) +
                    self.get_parameters('CMS.DAQ:CMSSW_VERSION', new)
               )
    def get_hlt_keys(self, run_numbers):
        return self.get_parameters('CMS.LVL0:HLT_KEY_DESCRIPTION', run_numbers)
    def get_run_keys(self, run_numbers):
        return self.get_parameters('CMS.DAQ:DAQ_RUN_KEY', run_numbers)
    def get_parameter(self, name, run_number):
        self._prepare_cursor(name)
        return self._execute_query(run_number)
    def get_parameters(self, name, run_numbers):
        self._prepare_cursor(name)
        return [self._execute_query(r) for r in run_numbers]
    def _prepare_cursor(self, name):
        query = """
            SELECT STRING_VALUE
            FROM CMS_RUNINFO.RUNSESSION_PARAMETER
            WHERE RUNNUMBER=:run_number and
            NAME='%s'
            """ % name
        self.logger.debug("Using SQL query: `%s'" % query)
        self.cursor.prepare(query)
    def _execute_query(self, run_number):
        self.logger.debug('Executing query for run %d ...' % run_number)
        self.cursor.execute(None, {'run_number': run_number})
        try:
            result = self.cursor.next()[0]
        except StopIteration:
            result = 'UNKNOWN'
        result = result.strip()
        self.logger.debug("Received `{0}'".format(result))
        return result
## RunInfo

    
#______________________________________________________________________________
if __name__ == '__main__':
    import user
    main()

