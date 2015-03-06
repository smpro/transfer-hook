#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Detects the macromerger end-of-run and reacts to it by closing the given run.

Jan Veverka, 13 November 2014, veverka@mit.edu

USAGE:
    ./eor.py eor.cfg

TODO:
    * Use rotating log files
    * Turn this into a deamon service
    * Avoid inspecting directories that have not changed
    * Factor out the call to isCompleteRun to merger
    * Add protection against missing MiniEoR files; options include:
        * Add the total sum over all BUs of the processed number of events 
          from upstream in the MiniEoR file
        * Obtain the list of BUs from somewhere and make sure we have
          all the MiniEoR files.
    * Factor out the Run class into a separate file
'''

__author__     = 'Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = []
__licence__    = 'Unknonw'
__version__    = '0.1.1'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'

import ConfigParser
import glob
import json
import logging
import os
import pprint
import socket
import sys
import time

from datetime import datetime, timedelta

import transfer.hook.bookkeeper as bookkeeper
import transfer.hook.metafile as metafile
import transfer.hook.runinfo as runinfo

from transfer.hook.macroeor import is_run_complete

logger = logging.getLogger(__name__)

_streams_to_ignore = ['EventDisplay', 'CalibrationDQM', 'Error']
_streams_to_dqm = ['DQMHistograms', 'DQM', 'DQMCalibration', 'CalibrationDQM']
_streams_to_ecal = ['EcalCalibration']
_streams_with_scalers = ['L1Rates', 'HLTRates']
_streams_to_exclude = _streams_to_ignore + \
                      _streams_to_dqm + \
                      _streams_to_ecal + \
                      _streams_with_scalers
#_______________________________________________________________________________
def main():
    '''
    Main entry point of execution.
    '''
    cfg = get_config()
    setup(cfg)
    logger.info(
        "Start closing runs between %d and %d in `%s' ..." % (
            cfg.runs_first, cfg.runs_last, cfg.input_path
        )
    )
    for iteration in range(1, cfg.max_iterations + 1):
        logger.info('=== ITERATION %d ===' % iteration)
        iterate(cfg)
        logger.info('Sleeping %d second(s) ...' % cfg.seconds_to_sleep)
        time.sleep(cfg.seconds_to_sleep)
    logger.info('Exiting with great success!')
## main


#_______________________________________________________________________________
class Config(object):
    '''
    Extracts configuration from the config file and holds its data
    in memory during run time.
    '''
    def __init__(self, filename=None):
        self.filename = filename
        self.general_dryrun = False
        self.max_iterations = 100000
        self.seconds_to_sleep = 20
        self.seconds_to_delay_run_closure = 60
        self.hours_to_wait_for_completion = 2.0
        self.json_suffix = None
        self.input_path = '/store/lustre/transfer'
        ## Set to None for logging to STDOUT
        self.logging_filename = 'eor.log'
        self.logging_level = logging.INFO
        self.logging_format = (r'%(asctime)s %(name)s %(levelname)s: '
                               r'%(message)s')
        self.runs_first = 233749 ## Begin of CRUZET Feb 2015
        self.runs_last  = 300000
        self.store_ini_area = '/store/lustre/mergeMacro'
        self.streams_to_exclude = _streams_to_exclude
        if filename:
            self._parse_config_file()
    ## __init__

    def _parse_config_file(self):
        parser = ConfigParser.ConfigParser()
        parser.read(self.filename)
        self.input_path = parser.get('Input', 'path')
        self.logging_filename = parser.get('Logging', 'filename')
        self.logging_level = getattr(logging, parser.get('Logging', 'level'))
        self.logging_format = parser.get('Logging', 'format', True)
    ## _parse_config_file
## Config


#_______________________________________________________________________________
def get_config():
    if len(sys.argv) == 1:
        return Config()
    elif len(sys.argv) == 2:
        return Config(sys.argv[1])
    else:
        logger.critical("Invalid args: %s" % str(sys.argv))
        print_usage()
        sys.exit(1)
## get_config


#_______________________________________________________________________________
def setup(cfg):
    '''
    Sets up the logging configuration.  Plan to apply configuration.
    '''
    ## Configure the logging
    logging.basicConfig(filename = cfg.logging_filename,
                        level    = cfg.logging_level,
                        format   = cfg.logging_format)
    bookkeeper._dry_run = cfg.general_dryrun
    ## Integration DB, will not be read by Tier0
    #bookkeeper._db_config = '.db.int2r.stomgr_w.cfg.py'
    ## Production DB, will be read by Tier0
    bookkeeper._db_config = '.db.rcms.stomgr_w.cfg.py'
    bookkeeper.setup()
    if not cfg.json_suffix:
        cfg.json_suffix = socket.gethostname()
    cfg.time_to_wait_for_completion = timedelta(
        hours=cfg.hours_to_wait_for_completion
    )
## setup


#_______________________________________________________________________________
def iterate(cfg):
    logger.info("Inspecting path `%s' ..." % cfg.input_path)
    runs = get_runs(cfg)
    for run in runs:
        if run.number == runs[-1].number and not run.stop_time():
            ## This is the last known run and looking at RunInfo,
            ## it hasn't stopped yet.
            logger.info('Run {0} is ongoing ...'.format(run.number))
            continue
        if run.is_complete2(cfg.streams_to_exclude, cfg.store_ini_area):
            logger.info('Closing run %d ...' % run.number)
        elif run.time_since_stop() > cfg.time_to_wait_for_completion:
            message = ('Run {0} INCOMPLETE FOR TOO LONG: {1}, closing ' +
                'it brute force!').format(run.number, time_since_stop)
            logger.warning(message)
        else:
            message = ('Run {0} incomplete {1} after stopping, waiting ' +
                'for it to complete ...').format(run.number, time_since_stop)
            logger.info(message)
            continue
        bookkeeper._run_number = run.number
        bookkeeper.main()
        run.close()
    logger.info("Finished inspecting path `%s'." % cfg.input_path)
## iterate


#_______________________________________________________________________________
def get_runs(cfg):
    runs = []
    dirnames = glob.glob(os.path.join(cfg.input_path, 'run*'))
    dirnames.sort()
    for dirname in dirnames:
        logger.debug("Inspecting `%s' ..." % dirname)
        try:
            run = Run(dirname, cfg.json_suffix)
            if cfg.runs_first and run.number < cfg.runs_first:
                logger.debug('Skipping run %d < %d because it is outside '
                              'of the range.' % (run.number, cfg.runs_first))
                continue
            if cfg.runs_last and run.number > cfg.runs_last:
                logger.debug('Skipping run %d > %d because it is outside '
                              'of the range.' % (run.number, cfg.runs_first))
                continue
            if run.is_closed():
                logger.debug('Skipping run %d because it is already closed.' %
                              run.number)
                continue
            logger.debug('Adding run %d to the processing.' % run.number)
            runs.append(run)
        except ValueError:
            logger.debug("Skipping `%s'." % dirname)
    return runs
# get_runs


#_______________________________________________________________________________
class Run(object):
    def __init__(self, path, suffix=socket.gethostname()):
        self.path = path
        self.suffix = suffix
        self.name = os.path.basename(self.path)
        self.number = int(self.name.replace('run', ''))
        eorname = '_'.join([self.name, 'ls0000', 'TransferEoR', suffix])
        #eormask = '_'.join([self.name, 'ls0000', 'TransferEoR', '*'])
        eormask = '_'.join([self.name, 'ls0000', 'TransferEoR', suffix])
        self.eorpath = os.path.join(self.path, eorname + '.jsn')
        self.eorglob = os.path.join(self.path, eormask + '.jsn')
    def is_complete(self, bu_count=15):
        eorfiles = self._get_minieor_files()
        if len(eorfiles) != bu_count:
            return False
        for eorfile in eorfiles:
            if not eorfile.is_run_complete():
                return False
        return True
    def is_complete2(self, streams_to_exclude, store_ini_area, threshold=1.0):
        is_run_complete(theInputDataFolder = self.path,
                        completeMergingThreshold = threshold,
                        outputEndName = self.suffix,
                        streamsToExclude = streams_to_exclude,
                        storeIniArea = store_ini_area)
        name = '_'.join([self.name, 'ls0000', 'MacroEoR', self.suffix])
        with open(os.path.join(self.path, name + '.jsn')) as source:
            data = json.load(source)
        return data['isComplete']
    def _get_minieor_files(self):
        mask = os.path.join(self.path, '*MiniEoR*.jsn')
        return [metafile.MiniEoRFile(f) for f in glob.glob(mask)]
    def is_open(self):
        '''
        A run is open when it is not closed.
        '''
        return not is_closed(self)
    def is_closed(self):
        '''
        A run is closed if the TransferEoR JSON exists.
        '''
        eorglob = glob.glob(self.eorglob)
        if eorglob:
            logger.debug(
                'Found TransferEoR file(s): %s' % pprint.pformat(eorglob)
                )
        else:
            logger.debug('Found no TransferEoR files.')
        return bool(eorglob)
    def close(self):
        '''
        Creates the TransferEoR file
        '''
        logger.info("Creating `%s' ..." % self.eorpath)
        with file(self.eorpath, 'a') as destination:
            pass
    def time_since_stop(self):
        '''
        Returns the datetime.timedelta object describing the time
        since the run stopped if it actually did stop. Othewise returns None.
        '''
        stop_time = self.stop_time()
        if stop_time:
            since_stop = datetime.utcnow() - stop_time
        else:
            since_stop = None
        return since_stop
    def stop_time(self):
        '''
        Returns the stop time as a datetime.datetime object if the run
        ended, returns None otherwise
        '''
        stop_time_string = runinfo.get_stop_time(self.number)
        if stop_time_string == 'UNKNOWN':
            stop_time = None
        else:
            time_string_format = '%m/%d/%y %I:%M:%S %p %Z'
            stop_time = datetime.strptime(stop_time_string, time_string_format)
        return stop_time
## Run


#_______________________________________________________________________________
def print_usage():
    print 'USAGE: ./eor.py [config]'
## print_usage


#_______________________________________________________________________________
if __name__ == '__main__':
    import user
    main()
# End of the module eor.py
