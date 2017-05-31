#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
TODO:
   * Query the DB more efficiently similar to ~/smpro/scripts/checkRun.pl
'''

import cx_Oracle
import datetime
import errno
import glob 
import json
import logging
import os
import pprint
import shlex
import shutil
import socket
import subprocess
import sys
import time
import multiprocessing
from multiprocessing.pool import ThreadPool

import smhook.bookkeeper as bookkeeper
import smhook.monitorRates as monitorRates
import smhook.fileQualityControl as fileQualityControl
import smhook.metafile as metafile
import smhook.eor as eor
import smhook.config as config
import smhook.databaseAgent as databaseAgent
import smhook.injectWorker as injectWorker
import smhook.copyWorker as copyWorker

from smhook.runinfo import RunInfo
from smhook.elasticSearch import elasticMonitor, esMonitorMapping, elasticMonitorUpdate

from datetime import datetime, timedelta, date
from optparse import OptionParser
from subprocess import call

import requests

#from Logging import getLogger
logger = logging.getLogger(__name__)

#______________________________________________________________________________
def main():
    '''
    Main entry point to execution.
    '''
    setup()

    _input_path = cfg.get('Input', 'path')
    _max_exceptions = cfg.getint('Misc','max_exceptions')
    _seconds_to_sleep = cfg.getint('Misc','seconds_to_sleep')
    _max_iterations = cfg.getfloat('Misc', 'max_iterations')
    _check_t0 = cfg.getboolean('Misc','t0_check')

    logger.info('input path is {0}'.format(_input_path))

    ## Call the elastic search mapping

    esServerUrl      = cfg.get('ElasticSearch','esServerUrl')
    esIndexName      = cfg.get('ElasticSearch','esIndexName')

    if not (esServerUrl == '' or esIndexName==''):
        esMonitorMapping(esServerUrl,esIndexName)

    ##Set up the database connection once, and then let them use the connection
    connection_bookkeeping = databaseAgent.makeConnection('bookkeeping')
    connection_filestatus  = databaseAgent.makeConnection('file_status')
    connection_t0status    = databaseAgent.makeConnection('file_status_T0')
    connection_l1_rates    = databaseAgent.makeConnection('l1_rates_write')

    #initilize the time that determines the T0 checks
    last_time_since_update = datetime.utcnow()

    caught_exception_count = 0
    iteration = 0
    while True:
        iteration += 1
        if iteration > _max_iterations:
            break
        logger.info('Start iteration {0} of {1} ...'.format(iteration,
                                                            _max_iterations))
        try:
            iterate()
        except Exception as e:
            caught_exception_count += 1
            logger.info(
                'Caught untreated exception number {0}'.format(
                    caught_exception_count
                    )
            )
            logger.exception(e)
            if caught_exception_count < _max_exceptions:
                logger.info(
                    'Will give up if I reach {0} exceptions.'.format(
                        _max_exceptions
                    )
                )
                logger.info('Trying to iterate again for now ...')
            else:
                logger.critical('Too many errors! Giving up ...')
                raise e
        logger.info('Sleeping {0} seconds ...'.format(_seconds_to_sleep))
        time.sleep(_seconds_to_sleep)
        
        time_since_update = datetime.utcnow() - last_time_since_update
        logger.info('Time since last T0 response check is {0} and the flag for T0 check is {1}'.format(time_since_update,_check_t0))
        if (time_since_update > timedelta(minutes=15) and _check_t0 == True):
            t0check_pool.apply_async(check_T0_response)
            last_time_since_update = datetime.utcnow()

    logger.info('Closing ECAL, DQM and Event Diplay transfer thread pools.')
    ecal_pool.close()
    dqm_pool.close()
    evd_pool.close()
    t0_pool.close()
    t0check_pool.close()

    logger.info('Joining ECAL, DQM and Event Display thransfer thread pools.')
    ecal_pool.join()
    dqm_pool.join()
    evd_pool.join()
    t0_pool.join()
    t0check_pool.join()    

    connection_bookkeeping.close()
    connection_filestatus.close()
    connection_t0status.close()
    connection_l1_rates.close()

## main()

#______________________________________________________________________________                                              
def check_T0_response():
    ''' Check for T0 response for files that are checked and transferred  '''
    logger.info('Preparing to check for T0 response for files that are transferred ...')
    injectWorker.findT0Files('checked',True)
    injectWorker.findT0Files('repacked',True)


#______________________________________________________________________________
def parse_args():
    parser = OptionParser(usage="usage: %prog [-h|--help] [-p|--path]")
    parser.add_option("-p", "--path",
                      action="store", dest="path",
                      help="path to watch for files to be transferred")
    ## Add an option to path were the files should be moved after being
    ## injected in the transfer
    options, args = parser.parse_args()
    if len(args) != 0:
        parser.error('You specified an invalid option - please use -h to '
                     'review the allowed options')
    if (options.path == None):
        parser.error('Please provide the path to watch')
    return options, args
## parse_args()

#______________________________________________________________________________
def setup():
    global log_and_maybe_exec
    global maybe_move
    global runinfo
    global ecal_pool
    global dqm_pool
    global evd_pool
    global lookarea_pool
    global t0_pool
    global t0check_pool
    global cfg
    cfg = config.config
    logger.info(
        'Using config file(s): %s ...' % ', '.join(cfg.filenames)
    )

    _dry_run = cfg.getboolean('Misc','dry_run')

    bookkeeper._dry_run = _dry_run
    bookkeeper.setup()

    runinfo = RunInfo(os.path.join(config.DIR, '.db.omds.runinfo_r.cfg.py'))
    if _dry_run:
        log_and_maybe_exec = log_and_do_not_exec
        maybe_move = mock_move_file_to_dir
    else:
        log_and_maybe_exec = log_and_exec
        maybe_move = move_file_to_dir
    ecal_pool     = ThreadPool(4)
    dqm_pool      = multiprocessing.Pool(5)
    evd_pool      = ThreadPool(4)
    lookarea_pool = ThreadPool(4)
    t0_pool       = multiprocessing.Pool(10)
    t0check_pool  = ThreadPool(1)
## setup()

#______________________________________________________________________________
def iterate():

    #Elastic Search Parameters
    esServerUrl      = cfg.get('ElasticSearch','esServerUrl')
    esIndexName      = cfg.get('ElasticSearch','esIndexName')

    ##HACK BY HAND
    #esServerUrl = 'http://es-cdaq.cms:9200'
    #esIndexName = 'merging_dv_write'

    # These all need to become globals read from config file, do that later
    path = cfg.get('Input', 'path')
    _eos_destination = cfg.get('Output','eos_path')
    _scratch_base    = cfg.get('Output','scratch_base')
    _error_base      = cfg.get('Output','error_base')
    _dqm_base        = cfg.get('Output','dqm_base')
    _ecal_base       = cfg.get('Output','ecal_base')
    _lookarea_base   = cfg.get('Output','lookarea_base')
    _evd_base        = cfg.get('Output','evd_base')
    _evd_eosbase     = cfg.get('Output','evd_eosbase')
    new_path_base    = cfg.get('Output', 'new_path_base')
    _checksum_status = cfg.getboolean('Misc','checksum_status')
    setup_label      = cfg.get('Input','setup_label')

    _streams_with_scalers = cfg.getlist('Streams','streams_with_scalars')
    _streams_to_ecal      = cfg.getlist('Streams','streams_to_ecal'     )
    _streams_to_evd       = cfg.getlist('Streams','streams_to_evd'      )
    _streams_to_dqm       = cfg.getlist('Streams','streams_to_dqm'      )
    _streams_to_lookarea  = cfg.getlist('Streams','streams_to_lookarea' )
    _streams_to_postpone  = cfg.getlist('Streams','streams_to_postpone' )
    _streams_to_ignore    = cfg.getlist('Streams','streams_to_ignore'   )
    _stream_type          = cfg.get('Streams','stream_type')
    _run_miniEOR          = cfg.getboolean('Misc','run_miniEOR')
    _total_machines       = cfg.get('Misc','total_machines')
    _machine_instance     = cfg.get('Misc','machine_instance')

    _dry_run = cfg.getboolean('Misc','dry_run')
    _renotify = cfg.getboolean('Misc','renotify')

    max_tier0_transfer_file_size = cfg.getint(
        'Output', 'maximum_tier0_transfer_file_size_in_bytes'
    )

    max_dqm_transfer_file_size = cfg.getint(
        'Output', 'maximum_dqm_transfer_file_size_in_bytes'
    )

    max_lookarea_transfer_file_size = cfg.getint(
        'Output', 'maximum_lookarea_transfer_file_size_in_bytes'
    )

    hostname = socket.gethostname()

    new_path = get_new_path(path, new_path_base)
    scratch_path = get_new_path(path, _scratch_base)
    rundirs, hltkeys = get_rundirs_and_hltkeys(path, new_path)

    if _run_miniEOR == True:
        # Just for the MiniEoR files
        for rundir in rundirs:
            logger.debug("Inspecting `%s' for EoR searching..." % rundir)
            jsns = sorted(glob.glob(os.path.join(rundir, '*EoR*.jsn')))
            if not jsns:
                continue

            rundir_basename  = os.path.basename(rundir) 
            run_number       = int(rundir_basename.replace('run', ''))
            new_rundir       = os.path.join(new_path    , rundir_basename)
            scratch_rundir   = os.path.join(scratch_path, rundir_basename)

            jsns.sort()
            run_key = runinfo.get_run_key(run_number)
            for jsn_file in jsns:
                if ('BoLS'  not in jsn_file and
                    'EoLS'  not in jsn_file and
                    'index' not in jsn_file and
                    'EoR' in jsn_file):
                    if run_key == 'TIER0_TRANSFER_OFF':
                        maybe_move(jsn_file, scratch_rundir,
                                   force_overwrite=True)
                    else:
                        maybe_move(jsn_file, new_rundir, force_overwrite=True)    
                    continue
    else:
        logger.info("Some other machine is moving the miniEoR files so I will ignore")

    # check if there are any stream directories 
    check_rundirs = []  
    for rundir in rundirs:
        if not (glob.glob(os.path.join(rundir, 'stream*'))): 
            rundir = rundir  
            check_rundirs.append(rundir)    
        else:
            for streamdir in glob.glob(os.path.join(rundir, 'stream*')):  
                check_rundirs.append(streamdir)  

    for rundir in check_rundirs:
        logger.debug("Inspecting `%s' ..." % rundir)
        logger.debug("Base name for the rundir is: %s " % os.path.basename(rundir))
        stream_basename = os.path.basename(rundir)
        stream_basename = stream_basename.replace('stream','')
        
        # Based on the stream type and stream name we will ingore stream directories (therefore files)
        
        if(_stream_type != "0"):

            isStreamDQMExpress = ("DQM" in stream_basename or "Express" in stream_basename or "Error" in stream_basename)
        
            if  (_stream_type == "onlyDQMExpress" and isStreamDQMExpress == False): 
                logger.debug("The directory {0} is ignored according to the configuration on this machine".format(stream_basename))
                continue
            elif(_stream_type == "noDQMExpressPhysics" and (isStreamDQMExpress == True)): 
                logger.debug("The directory {0} is ignored according to the configuration on this machine".format(stream_basename))
                continue
        
        
        if 'stream' in rundir:
            rundir_basename = os.path.basename(os.path.dirname(rundir)) 
        else:
            rundir_basename = os.path.basename(rundir) 

        # Make sure there are jsn files or else go to the next iteration
        jsn_parts = [rundir,'jsns','*.jsn']
        jsns = sorted(glob.glob(os.path.join(*jsn_parts)))
        logger.debug("The list of jsns are %s "  %jsns)

        # Find the recovery files which live under /recovery directory
        recovery_parts = [rundir, 'recovery', '*.jsn']
        recovery_jsns = sorted(glob.glob(os.path.join(*recovery_parts)))

        if not jsns and not recovery_jsns:
            continue # Skip the loop if there aren't any normal jsn files or recovery jsn files

        run_number = int(rundir_basename.replace('run', ''))
        logger.info('********** Run %d **********' % run_number)
        bookkeeper._run_number = run_number
        new_rundir       = os.path.join(new_path    , rundir_basename)
        rundir_bad       = os.path.join(rundir      , 'bad')
        new_rundir_bad   = os.path.join(new_rundir  , 'bad')
        scratch_rundir   = os.path.join(scratch_path, rundir_basename)
        error_rundir     = os.path.join(_error_base , rundir_basename)
        dqm_rundir_open  = os.path.join(_dqm_base   , rundir_basename, 'open')
        dqm_rundir       = os.path.join(_dqm_base   , rundir_basename)
        ecal_rundir_open = os.path.join(_ecal_base  , rundir_basename, 'open')
        ecal_rundir      = os.path.join(_ecal_base  , rundir_basename)
        evd_rundir       = os.path.join(_evd_base   , rundir_basename)
        evd_rundir_open  = os.path.join(_evd_base   , rundir_basename, 'open')
        evd_eosrundir    = os.path.join(_evd_eosbase, rundir_basename)
        lookarea_rundir_open = os.path.join(_lookarea_base  , rundir_basename,
                                            'open')
        lookarea_rundir      = os.path.join(_lookarea_base  , rundir_basename)
        run_key = runinfo.get_run_key(run_number)

        logger.info("********** Run %d is %s ***********" % (run_number, run_key))

        if not os.path.exists(scratch_rundir):
            mkdir(scratch_rundir)
            mkdir(os.path.join(scratch_rundir, 'bad'))
        
        # Define a directory for the recovery json files where they will be moved after they are monitored
        recorded_recovery_dir = os.path.join(rundir, 'recovery', 'recorded')
        
        for recovery_jsn in recovery_jsns:
                
            if (isStreamDQMExpress != True):
                if (int(recovery_jsn.split("_")[1].split("ls")[1])%int(_total_machines) != int(_machine_instance)):
                    continue

            if ('BoLS' not in recovery_jsn and
                'EoLS' not in recovery_jsn and
                'index' not in recovery_jsn):
                try:
                    settings_textI = open(recovery_jsn, "r").read()
                except IOError:
                    logger.warning("The json file %s is moved by the other machine!" % recovery_jsn)
                    continue
                try:
                    settings = json.loads(settings_textI)
                except ValueError:
                    logger.warning("The json file %s is corrupted!" % recovery_jsn)
                    maybe_move(recovery_jsn, new_rundir_bad, suffix='Corrupted')
                    continue
                if len(settings['data']) < 5:
                    logger.warning("Failed to parse `%s'!" % recovery_jsn)
                    maybe_move(recovery_jsn, scratch_rundir, force_overwrite=True)
                    continue
                inputEvents = int(settings['data'][0])
                fileName = str(settings['data'][3])
                fileSize = int(settings['data'][4])
                lumiSection = int(fileName.split('_')[1].strip('ls'))
                lumiSection = int(fileName.split('_')[1].strip('ls'))
                try:
                    streamName = str(fileName.split('_')[2].split('stream')[1])
                except Exception as e:                    
                    #run271983_ls0021_index000000_fu
                    streamName = str(fileName.split('_')[2].split('index')[1])
                    logger.exception(e)
                eventsNumber = int(settings['data'][1])
                errorEvents = int(settings['data'][2]) # BU/FU crash
                if inputEvents == 0:
                    logger.warning("There are 0 input events in this jsn %s" % recovery_jsn)
                    maybe_move(recovery_jsn, recorded_recovery_dir, force_overwrite=True)
                    continue
                events_built=inputEvents+errorEvents
                ##HACK FOR NOW
                fileQualityControl.fileQualityControl(fileName, run_number, lumiSection, streamName, fileSize, eventsNumber, eventsNumber, 0,0,0, events_built,True ) 
                #fileQualityControl.fileQualityControl(fileName, run_number, lumiSection, streamName, fileSize, events_built, eventsNumber, 0,0,0, events_built,True ) 
                logger.info("File quality control: recorded all events built as lost for file found in recovery area (jsn file {0}, data file {1})".format(recovery_jsn, fileName))
                if not os.path.exists(recorded_recovery_dir):
                    mkdir(recorded_recovery_dir)
                maybe_move(recovery_jsn, recorded_recovery_dir, force_overwrite=True)
        
        if not jsns:
            logger.info("No proper jsns found")
            continue

        if (not os.path.exists(new_rundir) and
            not run_key == 'TIER0_TRANSFER_OFF'):
            mkdir(new_rundir)
            mkdir(os.path.join(new_rundir, 'bad'))
            logger.info("Opening bookkeeping for run %d ..." % run_number)
            try:
                connection=databaseAgent.useConnection('bookkeeping')
                bookkeeper.open_run(connection.cursor(),setup_label)
                connection.commit()
            except cx_Oracle.IntegrityError:
                logger.warning(
                    'Bookkeeping for run %d already open!' % run_number
                )
        appversion = runinfo.get_cmssw_version(run_number)
        if appversion == 'UNKNOWN':
            logger.warning('The CMSSW version is UNKNOWN for run %d' %run_number)

        hlt_key = runinfo.get_hlt_key(run_number)
        # Sort JSON files by filename, implying also by lumi.
        jsns.sort()
        logger.info(
            "Processing {count} JSON file(s) in `{folder}':\n".format(
                count=len(jsns), folder=rundir
            ) + pprint.pformat([os.path.basename(f) for f in jsns])
        )
        for jsn_file in jsns:

            if (isStreamDQMExpress != True):                                                   
                 if (int(jsn_file.split("_")[1].split("ls")[1])%int(_total_machines) != int(_machine_instance)):
                     continue

            if ('BoLS' not in jsn_file and
                'EoLS' not in jsn_file and
                'index' not in jsn_file):

                try:
                    settings_textI = open(jsn_file, "r").read()
                except IOError:
                    logger.warning("The json file %s is moved by the other machine!" % jsn_file)
                    continue
                try:
                    settings = json.loads(settings_textI)
                except ValueError:
                    logger.warning("The json file %s is corrupted!" % jsn_file)
                    maybe_move(jsn_file, new_rundir_bad, suffix='Corrupted')
                    continue
                if len(settings['data']) < 5:
                    logger.warning("Failed to parse `%s'!" % jsn_file)
                    maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
                    continue
                inputEvents = int(settings['data'][0])

                if inputEvents == 0:
                    logger.warning("There are 0 input events in this jsn %s" % jsn_file)
                    maybe_move(jsn_file, new_rundir_bad, force_overwrite=True, suffix='ZeroInput')
                    #maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
                    continue

                eventsNumber = int(settings['data'][1])
                errorEvents = int(settings['data'][2]) # BU/FU crash
                fileName = str(settings['data'][3])
                if fileName == "":
                    logger.warning("There are no filenames specified in this jsn %s" % jsn_file)
                    maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
                    continue
                fileSize = int(settings['data'][4])
                lumiSection = int(fileName.split('_')[1].strip('ls'))

                try:
                    streamName = str(fileName.split('_')[2].split('stream')[1])
                except Exception as e:                    
                    streamName = str(fileName.split('_')[2].split('index')[1])
                    logger.exception(e)

                if ( _checksum_status ):
                    checksum_int = int(settings['data'][5]) 
                    checksum = format(checksum_int, 'x').zfill(8)    #making sure it is 8 digits
                else:
                    checksum = 0
                
                # FQC: File quality control numbers for the normal json files
                events_built = inputEvents+errorEvents # events lost to BU/FU crash are not included in inputEvents total!
                events_lost_checksum=0
                events_lost_cmssw=0
                events_lost_crash=errorEvents
                events_lost_oversized=0
                is_good_ls=True

                ## Here you might want to check if they exist first, this is only needed for elastic monitoring
                infoEoLS_1 = int(settings['data'][6])
                infoEoLS_2 = int(settings['data'][7])

                destination = str(settings['data'][9])
                logger.debug("Destination in the jsn file {0} is {1}".format(jsn_file,destination))

                if destination == "ErrorArea" or "Error" in streamName:
                    errorFiles = filter(None,fileName.split(","))
                    
                dat_parts = [rundir,'data',fileName]
                #dat_file = os.path.join(rundir, fileName)
                dat_file = os.path.join(*dat_parts)
                logger.debug("The hex format checksum of the file {0} is {1} ".format(dat_file, checksum))

                ### This is a protection for bubbles
                overwrite = False
                if (inputEvents == 0):
                    overwrite = True

                ###################################
                ##Now we start sending data around
                ###################################

                if streamName in _streams_with_scalers:
                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [inputEvents, eventsNumber, errorEvents, fileName, fileSize, infoEoLS_1, infoEoLS_2, int(time.time()*1000.), run_number, lumiSection, streamName,'rates', 1]
                        elasticMonitor(monitorData, esServerUrl, esIndexName, fileName, 5)
                    monitor_rates(jsn_file, fileName, esServerUrl, esIndexName)

                # Need to handle file quality control in a special case for cmssw errors
                if destination == "ErrorArea" or "Error" in streamName:
                    for nfile in range(0, len(errorFiles)):
                        events_lost_cmssw=events_built
                        logger.info("File quality control: recorded all events built as lost due to CMSSW error and moved to error run dir (file %s)" % errorFiles[nfile])
                        ##HACK FOR NOW
                        fileQualityControl.fileQualityControl(errorFiles[nfile], run_number, lumiSection, streamName, 0, eventsNumber, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls);       
                        #fileQualityControl.fileQualityControl(errorFiles[nfile], run_number, lumiSection, streamName, 0, events_built, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls);
                        dat_parts = [rundir, 'data',errorFiles[nfile]]
                        dat_file = os.path.join(*dat_parts)
                        maybe_move(dat_file, error_rundir, force_overwrite=overwrite)
                    maybe_move(jsn_file, error_rundir, force_overwrite=overwrite)
                    continue

                if streamName in _streams_to_dqm:
                    ## TODO: Use some other temporary directory instead of scrach
                    if (fileSize > max_dqm_transfer_file_size):
                        logger.warning(
                            "`{0}' too large ({1} > {2})! ".format(
                                dat_file, fileSize, max_dqm_transfer_file_size
                                ) +
                            "Moving it to bad area with the suffix `TooLarge' ..."
                            )
                        maybe_move(jsn_file, new_rundir_bad, force_overwrite=overwrite, suffix='TooLarge')
                        maybe_move(dat_file, new_rundir_bad, force_overwrite=overwrite, suffix='TooLarge')
                    else:
                        maybe_move(jsn_file, scratch_rundir, force_overwrite=overwrite)
                        maybe_move(dat_file, scratch_rundir, force_overwrite=overwrite)
                        jsn_file = jsn_file.replace(rundir, scratch_rundir)
                        jsn_file = jsn_file.replace('jsns/','')
                        dat_file = dat_file.replace(rundir, scratch_rundir)
                        dat_file = dat_file.replace('data/','')

                        #Elastic Monitor for DQM:
                        if not (esServerUrl=='' or esIndexName==''):
                            monitorData = [inputEvents, eventsNumber, errorEvents, fileName, fileSize, infoEoLS_1, infoEoLS_2, int(time.time()*1000.), run_number, lumiSection, streamName, 'DQM',1]
                            elasticMonitor(monitorData, esServerUrl, esIndexName, fileName, 5)

                        args = [dat_file, jsn_file, dqm_rundir_open, dqm_rundir, lookarea_rundir_open,lookarea_rundir,fileSize,max_lookarea_transfer_file_size,overwrite, fileName, esServerUrl, esIndexName]
                        dqm_pool.apply_async(double_p5_location, args)                        


                    continue

                if streamName in _streams_to_ecal:
                    ## TODO: Use some other temporary directory instead of
                    ## scratch
                    maybe_move(jsn_file, scratch_rundir, force_overwrite=overwrite)
                    maybe_move(dat_file, scratch_rundir, force_overwrite=overwrite)                                                            
                    jsn_file = jsn_file.replace(rundir, scratch_rundir)
                    jsn_file = jsn_file.replace('jsns/','')
                    dat_file = dat_file.replace(rundir, scratch_rundir)
                    dat_file = dat_file.replace('data/','')                              

                    #Elastic Monitor for ECAL:
                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [inputEvents, eventsNumber, errorEvents, fileName, fileSize, infoEoLS_1, infoEoLS_2, int(time.time()*1000.), run_number, lumiSection, streamName, 'EcalCalib',1]
                        elasticMonitor(monitorData, esServerUrl, esIndexName, fileName, 5)
          
                    args = [dat_file, jsn_file, ecal_rundir_open, ecal_rundir,overwrite, fileName, esServerUrl,esIndexName]
                    ecal_pool.apply_async(move_files, args)
                    continue

                if streamName in _streams_to_evd:
                    maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
                    maybe_move(dat_file, scratch_rundir, force_overwrite=True)

                    jsn_file = jsn_file.replace(rundir, scratch_rundir)
                    jsn_file = jsn_file.replace('jsns/','')
                    dat_file = dat_file.replace(rundir, scratch_rundir)
                    dat_file = dat_file.replace('data/','')

                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [inputEvents, eventsNumber, errorEvents, fileName, fileSize, infoEoLS_1, infoEoLS_2, int(time.time()*1000.), run_number, lumiSection, streamName, 'EventDisplay',1]
                        elasticMonitor(monitorData, esServerUrl, esIndexName, fileName, 5)
                    
                    args = [dat_file, jsn_file, evd_rundir_open, evd_rundir,
                            evd_eosrundir,overwrite, fileName, esServerUrl, esIndexName]
                    evd_pool.apply_async(copy_move_files, args)
                    continue


                if (run_key == 'TIER0_TRANSFER_OFF' or
                    streamName in (_streams_with_scalers +
                                   _streams_to_ignore)):

                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [inputEvents, eventsNumber, errorEvents, fileName, fileSize, infoEoLS_1, infoEoLS_2, int(time.time()*1000.), run_number, lumiSection, streamName, 'T0Off',1]
                        elasticMonitor(monitorData, esServerUrl, esIndexName, fileName, 5)

                    maybe_move(jsn_file, scratch_rundir, force_overwrite=overwrite)
                    maybe_move(dat_file, scratch_rundir, force_overwrite=overwrite)

                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [int(time.time()*1000.), 2]
                        elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, fileName, 5)
                    
                    continue


                ###########################
                ## Now doing the T0 Streams
                ###########################

                if (fileSize > max_tier0_transfer_file_size):
                    logger.warning(
                        "`{0}' too large ({1} > {2})! ".format(
                            dat_file, fileSize, max_tier0_transfer_file_size
                        ) +
                        "Moving it to bad area with the suffix `TooLarge' ..."
                    )
                    maybe_move(jsn_file, new_rundir_bad, force_overwrite=overwrite, suffix='TooLarge')
                    maybe_move(dat_file, new_rundir_bad, force_overwrite=overwrite, suffix='TooLarge')
                    continue

                ## If dry run do not try
                if _dry_run: 
                    logger.debug("Running dry_run mode, will not continue with the T0 stream copying or injection")
                    continue
                    
                number_of_files = 1
                if eventsNumber == 0:
                    logger.info("File '%s' has 0 events" % fileName)
                    number_of_files = 0

                    #Elastic Monitor for files with 0 events:
                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [inputEvents, eventsNumber, errorEvents, fileName, fileSize, infoEoLS_1, infoEoLS_2, int(time.time()*1000.), run_number, lumiSection, streamName, 'ZeroEvent',1]
                        elasticMonitor(monitorData, esServerUrl, esIndexName, fileName, 5)

                    maybe_move(jsn_file, new_rundir_bad, force_overwrite=overwrite, suffix='ZeroEvents')
                    maybe_move(dat_file, new_rundir_bad, force_overwrite=overwrite, suffix='ZeroEvents')

                    #Elastic Monitor for files with 0 events:
                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [int(time.time()*1000.), 2]
                        elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, fileName, 5)

                ## Always fill the bookkeeping table even before the actual transfers
                ## This is the only way to ensure we catch inconsistencies
                try:
                    # Do the bookkeeping
                    connection=databaseAgent.useConnection('bookkeeping')
                    bookkeeper.fill_number_of_files(
                        connection.cursor(), streamName, lumiSection, number_of_files
                    )
                    connection.commit()
                except cx_Oracle.IntegrityError:
                    print ('WARNING: Failed to insert bookkeeping for ' +
                           'run {0}, stream {1}, ls {2}: #files = {3}').format(
                               run_number, streamName, lumiSection,
                               number_of_files
                           )

                #Tranfser the files where the event number is not 0
                if (number_of_files == 1):
                    
                    #Elastic Monitor for T0:
                    if not (esServerUrl=='' or esIndexName==''):
                        monitorData = [inputEvents, eventsNumber, errorEvents, fileName, fileSize, infoEoLS_1, infoEoLS_2, int(time.time()*1000.), run_number, lumiSection, streamName, 'Tier0',1]
                        elasticMonitor(monitorData, esServerUrl, esIndexName, fileName, 5)

                    ## Inject worker inserts file and sets up the inject flag
                    if 'TransferTest' in setup_label:
                        inject_into_T0=False
                    else:
                        inject_into_T0=True
                      
                    file_id=injectWorker.insertFile(fileName, run_number, lumiSection, streamName, checksum, inject_into_T0)
                    if file_id is False or (file_id>0) is False:
                        logger.warning("injectWorker returned False for insertFile('%s',%d,%d,'%s','%s',inject_into_T0=%r)" % (fileName, run_number, lumiSection, streamName, checksum, inject_into_T0))
                        my_file_id, my_file_name, my_checksum = copyWorker.getFileInfo(-1,fileName,checksum)
                        logger.debug("my file_id {0}, filename {1}, checksum {2}".format(my_file_id,my_file_name,my_checksum))
                        if my_file_id is False:                        
                            logger.error("Setting the file_id to negative to continue with the operation until fixed...")
                            file_id = -1
                        else:
                            file_id = my_file_id
                            logger.info("Found a file ID # %d for getfileInfo('%s',%d,%d,'%s','%s',inject_into_T0=%r)" % (file_id, fileName, run_number, lumiSection, streamName, checksum, inject_into_T0))
                    else:
                        logger.info("injectWorker returned file ID # %d for insertFile('%s',%d,%d,'%s','%s',inject_into_T0=%r)" % (file_id, fileName, run_number, lumiSection, streamName, checksum, inject_into_T0))
  
                    maybe_move(dat_file, new_rundir, force_overwrite=overwrite)
                    maybe_move(jsn_file, new_rundir, force_overwrite=overwrite)
                      
                    new_file_path = os.path.join(new_rundir, fileName)

                    # Below we apply the copyFile call asynchronously
                    monitor_fqc=False
                    if streamName not in _streams_to_ignore:
                        monitor_fqc=True
                    if (fileSize > max_tier0_transfer_file_size):
                        events_lost_oversized=events_built

                    ##HACK FOR NOW
                    arguments_t0 = [file_id, fileName, checksum, new_file_path, _eos_destination, setup_label, monitor_fqc, jsn_file, run_number, lumiSection, streamName, fileSize, eventsNumber, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls, new_rundir_bad, esServerUrl, esIndexName, 5]
                    #arguments_t0 = [file_id, fileName, checksum, new_file_path, _eos_destination, setup_label, monitor_fqc, jsn_file, run_number, lumiSection, streamName, fileSize, events_built, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls, new_rundir_bad, esServerUrl, esIndexName, 5]

                    async_apply_result = t0_pool.apply_async(copyWorker.copyFile,arguments_t0)
                    # file quality control gets handled in the thread
                

        ## Move the bad area to new run dir so that we can check for run
        ## completeness
        for fname in sorted(glob.glob(os.path.join(rundir_bad, '*.jsn'))):
            logger.info("fname %s" %fname)
            try:
                jsn = metafile.File(fname)
                overwrite = True
                if jsn.type == metafile.Type.MacroMerger:
                    dat_path = jsn.path.replace('.jsn', '.dat')
                    maybe_move(jsn.path, new_rundir_bad, force_overwrite=overwrite)
                    maybe_move(dat_path, new_rundir_bad, force_overwrite=overwrite)
            except ValueError:
                logger.warning("Illegal filename `%s'!" % fname)

## iterate()


#______________________________________________________________________________
def get_new_path(path, new_base):
    '''
    Given the path to watch, returns the new path under which the files 
    being transferred are moved.
    '''
    head, tail = os.path.split(path)
    return os.path.join(head, new_base)
## get_new_path()


#______________________________________________________________________________
def mkdir(path):
    logger.info("Making directory `%s' ..." % path)
    os.mkdir(path)
## mkdir()


#______________________________________________________________________________
def get_rundirs_and_hltkeys(path, new_path):
    _run_number_max = cfg.getint('Misc','run_number_max')

    _run_number_max = 99999999999

    rundirs, runs, hltkeymap = [], [], {}
    full_list = sorted(glob.glob(os.path.join(path, 'run*')), reverse=True)

    for nf in range(0, min(len(full_list),150)):
        rundir = full_list[nf]
        run_number = get_run_number(rundir)
        
        if _run_number_max < run_number:
            continue

        new_rundir = os.path.join(new_path, rundir)
        if eor.Run(new_rundir).is_closed():
            continue
        rundirs.append(rundir)
        runs.append(run_number)
    results = runinfo.get_hlt_keys(runs)
    hltkeys = dict(zip(runs, results))
    rundirs.sort()
    runnumbers = [r.replace(os.path.join(path, 'run'), '') for r in rundirs]
    if len(runnumbers) == 0:
        logger.info("Found no run directories in `%s' ..." % path)
    else:
        logger.info(
            "Inspecting %d dir(s) in `%s' for runs %s to %s ..." % (
                len(rundirs), path, runnumbers[0], runnumbers[-1]
            )
        )
    logger.debug(pprint.pformat(runnumbers))
    logger.debug('HLT keys: ' + format_hltkeys(hltkeys))
    return rundirs, hltkeys
## get_rundirs_and_hltkeys()


#______________________________________________________________________________
def get_run_number(rundir):
    return int(os.path.basename(rundir).replace('run', ''))
## get_run_number

#______________________________________________________________________________
def monitor_rates(jsn_file, fileName, esServerUrl='', esIndexName=''):
    fname = jsn_file + 'data'
    fname = fname.replace('/jsns/','/data/')
    basename = os.path.basename(fname)
    try:
        logger.info('Inserting %s in WBM ...' % basename)
        monitorRates.monitorRates(fname,jsn_file)
        delay = get_time_since_modification(fname)
        max_delay = timedelta(
            seconds = cfg.getfloat('Misc', 'seconds_for_wbm_injection')
        )
        if delay < max_delay:
            logger.info(
                'Inserted {0} in WBM with a delay of {1}.'.format(fname, delay)
            )
        else:
            logger.warning(
                'Inserted {0} in WBM with too large of a delay: {1}!'.format(
                    fname, delay
                )
            )
        if not (esServerUrl=='' or esIndexName==''):
            monitorData = [int(time.time()*1000.), 2]
            elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, fileName, 5)
    except cx_Oracle.IntegrityError:
        logger.warning('DB record for %s already present!' %  basename)
## monitor_rates

#______________________________________________________________________________
def mock_move_file_to_dir(src, dst, force_overwrite=False, suffix=None,
                          eos=False,move=False):
    '''
    Prints a message about how it would move the file src to the directory dst
    if this was for real.
    '''
    ## Append the filename to the destination directory
    basename = os.path.basename(src)
    if suffix is not None:
        name, extension = os.path.splitext(basename)
        basename = name + suffix + extension
    dst = os.path.join(dst, basename)
    logger.debug("I would do: mv %s %s" % (src, dst))
    if eos:
        command =  ("xrdcp " + str(src) + " root://eoscms.cern.ch//" +
            str(dst))
        logger.debug("I woud do: %s" % command)
## mock_move_file_to_dir()

#______________________________________________________________________________
def move_file_to_dir(src, dst_dir, force_overwrite=False, suffix=None,
                     eos=False, move=True):
    '''
    Moves the file src to the directory dst_dir. Creates dst_dir if it doesn't
    exist.
    '''
    ## Append the filename to the destination directory
    src_dir , basename = os.path.split(src)
    if suffix is not None:
        name, extension = os.path.splitext(basename)
        basename = name + suffix + extension
    dst_path = os.path.join(dst_dir, basename)

    if not os.path.exists(src):
        logger.error("Source file `%s' doesn't exits!" % src)
        return
    if os.path.exists(dst_path):
        if os.path.samefile(src, dst_path):
            logger.info(
                "No need to do: mv %s %s, it is the same file." % (
                src, dst_path
                )
            )
            return
        elif force_overwrite:
            logger.info("Overwriting `%s'" % dst_path)
        else:
            raise RuntimeError, "Destination file `%s' exists!" % dst_path
    try:
        if eos:
            #do copy to eos
            command =  ("xrdcp " + str(src) + " root://eoscms.cern.ch//" +
                        str(dst_path))
            logger.info("Running `%s' ..." % command)
            os.system(command)
        else:
            if move:
                logger.info("Running `mv %s %s' ..." % (src, dst_path))
                shutil.move(src, dst_path)
            else:
                logger.info("Running `cp %s %s' ..." % (src, dst_path))
                shutil.copy(src, dst_path)

    except IOError as error:
        if error.errno == 2 and error.filename == dst_path:
            ## Directory dst_dir doesn't seem to exist. Let's create it.
            logger.info(
                "Failed moving `%s' b/c destination `%s' does not exist." % (
                    os.path.basename(src), dst_dir
                )
            )
            mkdir_with_parents(dst_dir)
            logger.info("Retrying `mv or xrdcp %s %s' ..." % (src, dst_path))
            if eos:
                #do copy to eos
                command =  ("xrdcp " + str(src) + " root://eoscms.cern.ch//" +
                            str(dst_path))
                logger.info("Running `%s' ..." % command)
                os.system(command)
            else:
                if move:
                    logger.info("Running `mv %s %s' ..." % (src, dst_path))
                    shutil.move(src, dst_path)
                else:
                    logger.info("Running `cp %s %s' ..." % (src, dst_path))
                    shutil.copy(src, dst_path)
                    
        else:
            logger.error(
                "errno: %d, filename: %s, message: %s" % (
                   error.errno, error.filename, error.strerror
                )
            )
            raise error
## move_file_to_dir()

#______________________________________________________________________________
def move_files(datFile, jsnFile, final_rundir_open, final_rundir, overwrite, fileName, esServerUrl='', esIndexName=''):
    try:
        # first move to open area
        maybe_move(datFile, final_rundir_open, force_overwrite=overwrite)
        maybe_move(jsnFile, final_rundir_open, force_overwrite=overwrite)
        # then move to the final area
        maybe_move(os.path.join(final_rundir_open,os.path.basename(datFile)),
                   final_rundir, force_overwrite=overwrite)
        maybe_move(os.path.join(final_rundir_open,os.path.basename(jsnFile)),
                   final_rundir, force_overwrite=overwrite)

        if not (esServerUrl=='' or esIndexName==''):
            monitorData = [int(time.time()*1000.), 2]
            elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, fileName, 5)
    except Exception as e:
        logger.exception(e)
## move_files()

#______________________________________________________________________________
def double_p5_location(datFile,jsnFile,copy_rundir_open, copy_rundir, move_rundir_open, move_rundir, fileSize, max_lookarea, overwrite, fileName, esServerUrl='', esIndexName=''):
    try:
        #first copy to open area dst1
        maybe_move(datFile, copy_rundir_open, force_overwrite=overwrite, suffix=None, eos=False, move=False)
        maybe_move(jsnFile, copy_rundir_open, force_overwrite=overwrite, suffix=None, eos=False, move=False)
        #then move to the final area for dst1
        maybe_move(os.path.join(copy_rundir_open,os.path.basename(datFile)),copy_rundir,force_overwrite=overwrite)
        maybe_move(os.path.join(copy_rundir_open,os.path.basename(jsnFile)),copy_rundir,force_overwrite=overwrite)

        # copying to lookarea only if the file size is less than 2 GB 
        logger.info("jsnfile is {0} and split is {1}".format(jsnFile,jsnFile.split("_")[1] ))
        if (int(os.path.basename(jsnFile).split("_")[1].split("ls")[1])%10 == 0):   
        #then move to open area dst2
            maybe_move(datFile, move_rundir_open,force_overwrite=overwrite)
            maybe_move(jsnFile, move_rundir_open,force_overwrite=overwrite)
        # then move to the final area
            maybe_move(os.path.join(move_rundir_open,os.path.basename(datFile)),move_rundir,force_overwrite=overwrite)
            maybe_move(os.path.join(move_rundir_open,os.path.basename(jsnFile)),move_rundir,force_overwrite=overwrite)               

        if not (esServerUrl=='' or esIndexName==''):
            monitorData = [int(time.time()*1000.), 2]
            elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, fileName, 5)

    except Exception as e:
        logger.exception(e)

## double_p5_location()

#______________________________________________________________________________
def copy_move_files(datFile, jsnFile, final_rundir_open, final_rundir,
                    final_eosrundir,overwrite, fileName, esServerUrl='', esIndexName=''):
    try:
        # first copy or move to the final area with the eos parameter
        maybe_move(datFile, final_eosrundir,force_overwrite=overwrite, eos=True)
        maybe_move(jsnFile, final_eosrundir,force_overwrite=overwrite, eos=True)

        # first move to open area in the nfs
        maybe_move(datFile, final_rundir_open,force_overwrite=overwrite,eos=False)
        maybe_move(jsnFile, final_rundir_open,force_overwrite=overwrite,eos=False)
        # then move to the final area in the nfs
        maybe_move(os.path.join(final_rundir_open,os.path.basename(datFile)),
                   final_rundir,force_overwrite=overwrite,eos=False)
        maybe_move(os.path.join(final_rundir_open,os.path.basename(jsnFile)),
                   final_rundir,force_overwrite=overwrite,eos=False)
        #maybe_move(datFile, final_rundir,eos=False)
        #maybe_move(jsnFile, final_rundir,eos=False)
        
        if not (esServerUrl=='' or esIndexName==''):
            monitorData = [int(time.time()*1000.), 2]
            elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, fileName, 5)

    except Exception as e:
        logger.exception(e)
## copy_move_files()

#______________________________________________________________________________
def log_and_exec(args, print_output=False):
    ## Make sure all arguments are strings; cast integers.
    args = map(str, args)
    logger.info("Running `%s' ..." % ' '.join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if print_output:
        if out:
            logger.info('STDOUT: ' + str(out))
        if err:
            logger.info('STDERR: ' + str(err))
    return out, err
## log_and_exec()


#______________________________________________________________________________
def log_and_do_not_exec(args, print_output=False):
    ## Make sure all arguments are strings; cast integers.
    args = map(str, args)
    log("I would run:\n  %s" % ' '.join(args))
## log_and_do_not_exec()


#______________________________________________________________________________
def need_to_retransfer(out):
    for status in _file_status_list_to_retransfer:
        if status.lower() in out.lower():
            return True
    return False
## need_to_retransfer()


#______________________________________________________________________________
def log(msg, newline=True):
    msg = "%s: %s" % (strftime(), msg)
    if newline:
        logger.info(msg)
    else:
        logger.info(msg)
## log()


#______________________________________________________________________________
def strftime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
## strftime()


#______________________________________________________________________________
## http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def mkdir_with_parents(path):
    '''
    Create a directory, including parents when needed.  No error when exists.
    '''
    try:
        logger.info("Making directory `%s' (incl. parents) ..." % path)
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            logger.info("Directory `%s' already exists!" % path)
            pass
        else:
            raise
## mkdir_with_parents


#______________________________________________________________________________
def format_hltkeys(hltkeys, max_runs=4, indent=4):
    rows = []
    run_map = invert(hltkeys)
    # t = (<hltkey>, [<run_1>, <run_2>, ..., <run_n>])
    for hltkey, runs in sorted(run_map.items(), key=lambda t: min(t[1])):
        if len(runs) == 1:
            row = hltkey + ': run ' + str(runs[0])
        elif len(runs) < 5:
            row = hltkey + ': runs ' + ', '.join(map(str, runs))
        else:
            row = '{hltkey}: {count} runs between {first} and {last}'.format(
                hltkey=hltkey, count=len(runs), first=min(runs), last=max(runs)
            )
        rows.append(row)
    return ('\n' + indent * ' ').join(rows)
## format_hltkeys


#______________________________________________________________________________
def invert(mapping):
    '''
    Returns an inverse of the given mapping.
    http://stackoverflow.com/questions/483666/python-reverse-inverse-a-mapping
    '''
    inverse_mapping = {}
    for key, value in mapping.iteritems():
        inverse_mapping.setdefault(value, []).append(key)
    return inverse_mapping
## invert_mapping

#______________________________________________________________________________
def get_time_since_modification(filename):
    '''
    Returns the timedelta object giving the time since the last modification
    of the file given by filename.
    '''
    m_time_stamp = int(os.stat(filename).st_mtime)
    m_utc_date_time = datetime.utcfromtimestamp(m_time_stamp)
    return datetime.utcnow() - m_utc_date_time
#______________________________________________________________________________                                                                                                                                                     
                                                                                                  
if __name__ == '__main__':

    main(_path)
