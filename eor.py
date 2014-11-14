#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Detects the macromerger end-of-run and reacts to it by closing the given run.

Jan Veverka, 13 November 2014, veverka@mit.edu

USAGE:
    ./eor.py eor.cfg

TODO:
    * Add protection against missing MiniEoR files; options include:
        * Add the total sum over all BUs of the processed number of events 
          from upstream in the MiniEoR file
        * Obtain the list of BUs from somewhere and make sure we have
          all the MiniEoR files.
    * Suppress less important messages INFO -> DEBUG
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
import logging
import os
import sys

import bookkeeper
import metafile


#_______________________________________________________________________________
def main():
    '''
    Main entry point of execution.
    '''
    cfg = get_config()
    setup(cfg)
    logging.info('Start processing ...')
    process(cfg)
    logging.info('Exiting with great success!')
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
        self.input_path = '/store/lustre/mergeMacro'
        ## Set to None for logging to STDOUT
        self.logging_filename = 'eor.log'
        self.logging_level = logging.DEBUG
        self.logging_format = (r'%(asctime)s %(name)s %(levelname)s: '
                               r'%(message)s')
        self.runs_first = 229713
        self.runs_last  = 300000
        if filename:
            self._parse_config_file()
    ## __init__

    def _parse_config_file():
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
        logging.error("Invalid args: %s" % str(sys.argv))
        print_usage()
        sys.exit(1)
## get_config


#_______________________________________________________________________________
def setup(cfg):
    '''
    Sets up the logging configuration.  Plan to apply configuration.
    '''
    logging.basicConfig(filename = cfg.logging_filename,
                        level    = cfg.logging_level,
                        format   = cfg.logging_format)
    bookkeeper._dry_run = cfg.general_dryrun
## setup


#_______________________________________________________________________________
def process(cfg):
    logging.info('Processing path %s ...' % cfg.input_path)
    for run in get_runs(cfg):
        if run.is_complete():
            logging.info('Closing run %d ...' % run.number)
            bookkeeper._run_number = run.number
            bookkeeper.main()
        else:
            logging.warning('Run %d is incomplete!' % run.number)
    logging.info('Finished processing path %s.' % cfg.input_path)
## process


#_______________________________________________________________________________
def get_runs(cfg):
    runs = []
    dirnames = glob.glob(os.path.join(cfg.input_path, 'run*'))
    dirnames.sort()
    for dirname in dirnames:
        logging.info("Inspecting `%s' ..." % dirname)
        try:
            run = Run(dirname)
            if cfg.runs_first and run.number < cfg.runs_first:
                continue
            if cfg.runs_last and run.number > cfg.runs_last:
                continue
            logging.info('Adding run %d to the processing.' % run.number)
            runs.append(run)
        except ValueError:
            logging.info("Skipping `%s'." % dirname)
    return runs
# get_runs


#_______________________________________________________________________________
class Run(object):
    def __init__(self, path):
        self.dir = path
        self.number = int(os.path.basename(path).replace('run', ''))
        
    def is_complete(self, bu_count=15):
        eorfiles = self._get_minieor_files()
        if len(eorfiles) != bu_count:
            return False
        for eorfile in eorfiles:
            if not eorfile.is_run_complete():
                return False
        return True
        
    def _get_minieor_files(self):
        mask = os.path.join(self.dir, '*MiniEoR*.jsn')
        return [metafile.MiniEoRFile(f) for f in glob.glob(mask)]
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
