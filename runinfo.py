#!/bin/env python
# -*- coding: utf-8 -*-
## Use of str.foramt requires Python version >= 2.6.
import logging
import sys

import cx_Oracle

import transfer.hook.config as config

logger = logging.getLogger(__name__)

## Default configuration
_driver_defaults = config.Config(
    logging_level = logging.WARNING,
    db_config_file = '.db.omds.runinfo_r.cfg.py',
    run_numbers = [
        229221,
    ]
)

_first_run_with_new_cmssw_version_name_tag = 231017


#______________________________________________________________________________
def main():
    cfg = _driver_defaults
    logging.basicConfig(level = cfg.logging_level)
    logger = logging.getLogger(__name__)
    if sys.argv[1:]:
        cfg.run_numbers = map(int, sys.argv[1:])
    driver = Driver(cfg)
    driver.main()
    logger.info('End')
## main


#______________________________________________________________________________
class Driver(object):
    def __init__(self, cfg=_driver_defaults):
        self.logger = logging.getLogger(type(self).__module__ + '.' +
                                        type(self).__name__)
        self.cfg = cfg
        self._setup()
    def _setup(self):
        if hasattr(self.cfg, 'run_numbers'):
            self.run_numbers = self.cfg.run_numbers
        else:
            self.run_numbers = []
        self.runinfo = RunInfo(self.cfg.db_config_file)
    def main(self):
        self.dump_cmssw_versions()
        self.dump_hlt_keys()
        self.dump_run_keys()
        self.runinfo.connection.close()
    def dump_cmssw_versions(self):
        print 'Run    CMSSW Version'
        results = self.runinfo.get_cmssw_versions(self.run_numbers)
        for run_number, result in zip(self.run_numbers, results):
            print run_number, result
    def dump_hlt_keys(self):
        print 'Run    HLT Key'
        results = self.runinfo.get_hlt_keys(self.run_numbers)
        for run_number, result in zip(self.run_numbers, results):
            print run_number, result
    def dump_run_keys(self):
        print 'Run    Run Key'
        results = self.runinfo.get_run_keys(self.run_numbers)
        for run_number, result in zip(self.run_numbers, results):
            print run_number, result


#______________________________________________________________________________
class RunInfo(object):
    def __init__(self, db_config_file):
        self.logger = logging.getLogger(type(self).__module__ + '.' +
                                        type(self).__name__)
        self.db_config_file = db_config_file
        self._setup()
        self._define_custom_getters()
    def _setup(self):
        db = config.load(self.db_config_file)
        self.logger.info('Connecting to %s@%s ...' % (db.reader, db.sid))
        self.connection = cx_Oracle.connect(db.reader, db.phrase, db.sid)
        self.cursor = self.connection.cursor()
    def _define_custom_getters(self):
        '''
        For each name, key pair, defines a getters get_<name>(run) and
        get_<name>s(runs) that return the values of the Run Info parameter(s)
        with the given key and run(s).  For example, for the pair
        ('run_key', 'CMS.DAQ:RUN_KEY'), the getters get_run_key(run_number) and
        get_run_keys(run_numbers) are defined that return the values of
        Run Info parameters with the key 'CMS.DAQ:RUN_KEY' for the run(s)
        given by run_number(s).
        '''
        for name, key in [
            ('run_key'          , 'CMS.DAQ:RUN_KEY'             ),
            ('daqfm_hlt_key'    , 'CMS.DAQ:HLT_KEY'             ),
            ('lvl0fm_hlt_key'   , 'CMS.LVL0:HLT_KEY_DESCRIPTION'),
            ('old_cmssw_version', 'CMS.DAQ:DAQ_CMSSW_VERSION_T' ),
            ('new_cmssw_version', 'CMS.DAQ:CMSSW_VERSION'       ),
        ]:
            getter_name      = 'get_' + name
            list_getter_name = 'get_' + name + 's'
            getter      = self._make_getter     (key)
            list_getter = self._make_list_getter(key)
            setattr(self, getter_name     , getter)
            setattr(self, list_getter_name, list_getter)
    def get_hlt_key(self, run_number):
        ## By default, use the info written by DAQ FM to get the HLT key
        hlt_key = self.get_daqfm_hlt_key(run_number)
        if hlt_key == 'UNKNOWN':
            ## Try to use the info from LevelZero FM (works e.g. for
            ## run 229221, for which DAQ FM info is absent)
            hlt_key = self.get_lvl0fm_hlt_key(run_number)
        return hlt_key
    def get_hlt_keys(self, run_numbers):
        ## By default, use the info written by DAQ FM to get the HLT key
        hlt_keys = self.get_daqfm_hlt_keys(run_numbers)
        for i, (hlt_key, run_number) in enumerate(zip(hlt_keys, run_numbers)):
            if hlt_key == 'UNKNOWN':
                ## Try to use the info from LevelZero FM (works e.g. for
                ## run 229221, for which DAQ FM info is absent)
                hlt_keys[i] = self.get_lvl0fm_hlt_key(run_number)
        return hlt_keys
    def get_cmssw_version(self, run_number):
        if run_number < _first_run_with_new_cmssw_version_name_tag:
            get = self.get_old_cmssw_version
        else:
            get = self.get_new_cmssw_version
        return get(run_number)
    def get_cmssw_versions(self, run_numbers):
        old_runs, new_runs = [], []
        for r in run_numbers:
            if r < _first_run_with_new_cmssw_version_name_tag:
                old_runs.append(r)
            else:
                new_runs.append(r)
        old_cmssw_versions = self.get_old_cmssw_versions(old_runs)
        new_cmssw_versions = self.get_new_cmssw_versions(new_runs)
        result_map = dict(zip(old_runs + new_runs,
                              old_cmssw_versions + new_cmssw_versions))
        return [result_map[r] for r in run_numbers]
    def _make_getter(self, name):
        def getter(run_number):
            return self.get_parameter(name, run_number)
        return getter
    def _make_list_getter(self, name):
        def list_getter(run_numbers):
            return self.get_parameters(name, run_numbers)
        return list_getter
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


#______________________________________________________________________________
if __name__ == '__main__':
    import user
    main()

