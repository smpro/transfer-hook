#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Detects the macromerger end-of-run and reacts to it by closing the given run.

Jan Veverka, 13 November 2014, veverka@mit.edu

USAGE:
    service smeord start

TODO:

'''

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

import smhook.bookkeeper as bookkeeper
import smhook.config as config
import smhook.metafile as metafile
import smhook.runinfo as runinfo

from smhook.macroeor import is_run_complete

logger = logging.getLogger(__name__)
_running = True

_streams_to_ignore = ['EventDisplay', 'CalibrationDQM', 'Error']
_streams_to_dqm = ['DQMHistograms', 'DQM', 'DQMCalibration', 'CalibrationDQM']
_streams_to_ecal = ['EcalCalibration']
_streams_with_scalars = ['L1Rates', 'HLTRates']
_streams_to_exclude = _streams_to_ignore + \
                      _streams_to_dqm + \
                      _streams_to_ecal + \
                      _streams_with_scalars

#_______________________________________________________________________________
def main():
    '''
    Main entry point of execution.
    '''
    logger.info("Welcome to eor.main()")
    cfg = get_config()
    # Configure the logging
    logging.basicConfig(filename = cfg.logging_filename,
                        level    = cfg.logging_level,
                        format   = cfg.logging_format)
    run(cfg)
## main

#_______________________________________________________________________________
def run():
    global _running
    logger.info('Running ...')
    cfg = get_config()
    setup(cfg)
    logger.info(
        "Start closing runs upto %d in `%s' ..." % (
            cfg.runs_last, cfg.input_path
        )
    )
    iteration = 0
    while _running:
        iteration += 1
        if iteration >= cfg.max_iterations:
            _running = False
        logger.info('Iteration {0} of {1} ...'.format(iteration,
                                                      cfg.max_iterations))
        iterate(cfg)
        logger.info('Sleeping %d second(s) ...' % cfg.seconds_to_sleep)
        time.sleep(cfg.seconds_to_sleep)
    logger.info('Stopped, exiting ...')

#_______________________________________________________________________________
def terminate():
    global _running
    logger.info('Received the terminate signal.')
    _running = False

#_______________________________________________________________________________
class Config(object):
    '''
    Extracts configuration from the config file and holds its data
    in memory during run time.
    '''
    def __init__(self, filename=None):
        self.filename = filename
        self.general_dryrun = False
        self.max_iterations = float('inf')
        self.seconds_to_sleep = 20
        self.seconds_to_delay_run_closure = 60
        self.hours_to_wait_for_completion = 2.0
        self.json_suffix = None
        self.input_path = '/opt/transfers/mock_directory/transfer'
        self.runs_last  = 300000
        self.store_ini_area = '/opt/transfers/mock_directory/mergeMacro'
        self.streams_to_exclude = _streams_to_exclude
        if filename:
            self._parse_config_file()
    ## __init__

    def _parse_config_file(self):
        parser = ConfigParser.ConfigParser()
        parser.read(self.filename)
        self.input_path = parser.get('Input', 'path')
    ## _parse_config_file
## Config


#_______________________________________________________________________________
def get_config():
    return Config()
## get_config


#_______________________________________________________________________________
def setup(cfg):
    '''
    Sets up the logging configuration.  Plan to apply configuration.
    '''
    ## Get global configurations from the config module
    myconfig = config.config
    logger.info(
        'Using config file(s): %s ...' % ', '.join(myconfig.filenames)
    )
    myconfig.getlist = getlist_from_config(myconfig)
    cfg.input_path = myconfig.get('eor', 'input_path')
    cfg.store_ini_area = myconfig.get('eor', 'store_ini_area')
    cfg.general_dryrun = myconfig.getboolean('eor', 'general_dryrun')
    cfg.max_iterations = myconfig.getfloat('eor', 'max_iterations')
    cfg.seconds_to_sleep = myconfig.getfloat('eor', 'seconds_to_sleep')
    cfg.seconds_to_delay_run_closure = myconfig.getfloat(
        'eor', 'seconds_to_delay_run_closure'
    )
    cfg.hours_to_wait_for_completion = myconfig.getfloat(
        'eor', 'hours_to_wait_for_completion'
    )
    cfg.runs_last = myconfig.getint('eor', 'runs_last')
    cfg.streams_to_exclude = []
    for stream_list in myconfig.getlist('eor', 'streams_to_exclude'):
        cfg.streams_to_exclude.extend(
            myconfig.getlist('Streams', stream_list)
        )
    bookkeeper._dry_run = cfg.general_dryrun
    ## Integration DB, will not be read by Tier0
    #bookkeeper._db_config = '.db.int2r.stomgr_w.cfg.py'
    ## Production DB, will be read by Tier0
    bookkeeper._db_config = myconfig.get('eor', 'db_config_path')
    logger.debug("Using `%s' for DB credentials ..." % bookkeeper._db_config)
    bookkeeper.setup()
    bookkeeper._input_dir = cfg.input_path
    bookkeeper._excluded_streams = cfg.streams_to_exclude
    bookkeeper.setup()
    logger.debug("Using %s for input directory ..." % bookkeeper._input_dir)
    logger.debug("Using %s for streams to exclude ..." % bookkeeper._excluded_streams)

    if not cfg.json_suffix:
        cfg.json_suffix = socket.gethostname()
    cfg.time_to_wait_for_completion = timedelta(
        hours=cfg.hours_to_wait_for_completion
    )
## setup


#_______________________________________________________________________________
def iterate(cfg):
    global _running
    logger.info("Inspecting path `%s' ..." % cfg.input_path)
    runs = get_runs(cfg)
    for run in runs:
        if _running is False:
            logger.info('Stopping loop over runs ...')
            break
        time_since_stop = run.time_since_stop()
        logger.info('Time since stop is {0}'.format(time_since_stop))
        #if run.number == runs[-1].number and not time_since_stop:
        if run.number == runs[0].number and not time_since_stop:
            ## This is the last known run and looking at RunInfo,
            ## it hasn't stopped yet.
            logger.info('Run {0} is ongoing ...'.format(run.number))
            continue
        if run.is_complete2(cfg.streams_to_exclude, cfg.store_ini_area):
            logger.info('Closing run %d ...' % run.number)
        elif time_since_stop > cfg.time_to_wait_for_completion:
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
    dirnames.sort(reverse=True)
    #for dirname in dirnames:
    for nf in range(0, min(len(dirnames),50)):
        dirname = dirnames[nf]
        logger.debug("Inspecting `%s' ..." % dirname)
        try:
            run = Run(dirname, cfg.json_suffix)
            filename = os.path.join(dirname,'*ls0000*TransferEoR*.jsn')
            if glob.glob(os.path.join(dirname,'*ls0000*TransferEoR*.jsn')):
                logger.debug('Skipping run because Transfer EoR exists')
                continue
            if cfg.runs_last and run.number > cfg.runs_last:
                logger.debug('Skipping run %d > %d because it is outside '
                              'of the range.' % (run.number, cfg.runs_last))
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
def getlist_from_config(self):
    def getlist(section, option):
        return map(str.strip, self.get(section, option).split(','))
    return getlist

#_______________________________________________________________________________
def print_usage():
    print 'USAGE: ./eor.py [config]'
## print_usage


#_______________________________________________________________________________
if __name__ == '__main__':
    import user
    main()
# End of the module eor.py
