#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
TODO:
   * Include lumi bookkeeping. (first prototype done 2014/10/09)
   * Notify Tier0 about the open runs, insert runs with open status flag
   * Add checksums
   * Automate the CMSSW version retrieval (done 2014/12/08)
   * Use the logging module for logging
   * Turn this into a deamon service
   * Puppet-ize
   * pip-ify
   * Move to using the log file as the transfer test instead inject*.pl
   * Query the DB more efficiently similar to ~/smpro/scripts/checkRun.pl
   * Only process each JSON file once. Move both the JSON and data to a new
     location first. Then inject it in the transfer.
   * Move MiniEoR and bad files in the transfer area
   * Make sure to move the MiniEoR files after the meta data files to
     prevent closing the run with wrongly low lumi count. The EOR
     needs all the meta data files to do the bookkeeping right.
     see https://hypernews.cern.ch/HyperNews/CMS/get/smops/804.html
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
import smhook.metafile as metafile
import smhook.eor as eor
import smhook.config as config

from optparse import OptionParser
from subprocess import call

from smhook.runinfo import RunInfo
from smhook.config import Config

__author__     = 'Lavinia Darlea, Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = ['Dirk Hufnagel', 'Guillelmo Gomez-Ceballos']

__licence__    = 'Unknonw'
__version__    = '0.2.3'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'

#from Logging import getLogger
logger = logging.getLogger(__name__)


## Defualt is False, set this to True if you want to re-transfer.

#_db_config = '.db.int2r.stomgr_w.cfg.py' # integration
#_db_config = '/opt/transfers/.db.rcms.stomgr_w.cfg.py' # production


#______________________________________________________________________________
def main():
    '''
    Main entry point to execution.
    '''
    setup()

    _input_path = cfg.get('Input', 'path')
    _max_exceptions = cfg.getint('Misc','max_exceptions')
    _seconds_to_sleep = cfg.getint('Misc','seconds_to_sleep')
    _max_iterations = float('inf')

    logger.info('input path is {0}'.format(_input_path))
    caught_exception_count = 0
    iteration = 0
    while True:
        iteration += 1
        if iteration > _max_iterations:
            break
        logger.info('Start iteration {0} of {1} ...'.format(iteration,_max_iterations))
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

    logger.info('Closing ECAL and DQM thransfer thread pools.')
    ecal_pool.close()
    dqm_pool.close()
    logger.info('Joining ECAL and DQM thransfer thread pools.')
    ecal_pool.join()
    dqm_pool.join()
## main()


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
    global cfg
    cfg = config.config

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
    ecal_pool = ThreadPool(4)
    dqm_pool = ThreadPool(4)
    
## setup()

#______________________________________________________________________________
def iterate():
    path = cfg.get('Input', 'path')
    _scratch_base = cfg.get('Output','scratch_base')
    _dqm_base = cfg.get('Output','dqm_base')
    _ecal_base = cfg.get('Output','ecal_base')

    db_config = cfg.get('Bookkeeping', 'db_config')
    new_path_base = cfg.get('Output', 'new_path_base')
    db_cred = config.load(db_config)
    connection = cx_Oracle.connect(db_cred.db_user, db_cred.db_pwd, 
                                   db_cred.db_sid)
    cursor = connection.cursor()

    _streams_with_scalers = map(str.strip, cfg.get('Streams','streams_with_scalars').split(','))
    _streams_to_ecal      = map(str.strip, cfg.get('Streams','streams_to_ecal').split(','))
    _streams_to_dqm       = map(str.strip, cfg.get('Streams','streams_to_dqm').split(','))
    _streams_to_postpone  = map(str.strip, cfg.get('Streams','streams_to_postpone').split(','))
    _streams_to_ignore    = map(str.strip, cfg.get('Streams','streams_to_ignore').split(','))

    _injectscript = cfg.get('Input', 'injectscript')

    _renotify = cfg.getboolean('Misc','renotify')

    max_tier0_transfer_file_size = cfg.getint(
        'Output', 'maximum_tier0_transfer_file_size_in_bytes'
    )

    hostname = socket.gethostname()

    new_path = get_new_path(path, new_path_base)
    scratch_path = get_new_path(path, _scratch_base)
    rundirs, hltkeys = get_rundirs_and_hltkeys(path, new_path)
    for rundir in rundirs:
        logger.debug("Inspecting `%s' ..." % rundir)
        jsns = glob.glob(os.path.join(rundir, '*.jsn'))
        if not jsns:
            continue
        run_number = int(os.path.basename(rundir).replace('run', ''))
        logger.info('********** Run %d **********' % run_number)
        bookkeeper._run_number = run_number
        new_rundir = os.path.join(new_path, os.path.basename(rundir))
        rundir_bad = os.path.join(rundir, 'bad')
        new_rundir_bad = os.path.join(new_rundir, 'bad')
        scratch_rundir = os.path.join(scratch_path, os.path.basename(rundir))
        dqm_rundir_open  = _dqm_base  + "/" + os.path.basename(rundir) + "/open"
        dqm_rundir       = _dqm_base  + "/" + os.path.basename(rundir)
        ecal_rundir_open = _ecal_base + "/" + os.path.basename(rundir) + "/open"
        ecal_rundir      = _ecal_base + "/" + os.path.basename(rundir)
        run_key = runinfo.get_run_key(run_number)
        if not os.path.exists(scratch_rundir):
            mkdir(scratch_rundir)
            mkdir(os.path.join(scratch_rundir, 'bad'))
        if (not os.path.exists(new_rundir) and
            not run_key == 'TIER0_TRANSFER_OFF'):
            mkdir(new_rundir)
            mkdir(os.path.join(new_rundir, 'bad'))
            logger.info("Opening bookkeeping for run %d ..." % run_number)
            try:
                bookkeeper.open_run(cursor)
                connection.commit()
            except cx_Oracle.IntegrityError:
                logger.warning(
                    'Bookkeeping for run %d already open!' % run_number
                )
        appversion = runinfo.get_cmssw_version(run_number)
        if appversion == 'UNKNOWN':
            appversion = get_cmssw_version(run_number)
        #hlt_key = hltkeys[run_number]
        hlt_key = runinfo.get_hlt_key(run_number)
        # Sort JSON files by filename, implying also by lumi.
        jsns.sort()
        # Move the EoR files (ls0000) to the end.
        jsns.sort(key=lambda x: 'EoR' in x)
        logger.info(
            "Processing {count} JSON file(s) in `{folder}':\n".format(
                count=len(jsns), folder=rundir
            ) + pprint.pformat([os.path.basename(f) for f in jsns])
        )
        for jsn_file in jsns:
            if ('BoLS' not in jsn_file and
                'EoLS' not in jsn_file and
                'index' not in jsn_file):
                if 'EoR' in jsn_file:
                    if run_key == 'TIER0_TRANSFER_OFF':
                        maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
                    else:
                        maybe_move(jsn_file, new_rundir, force_overwrite=True)
                    continue
                settings_textI = open(jsn_file, "r").read()
                settings = json.loads(settings_textI)
                if len(settings['data']) < 5:
                    logger.warning("Failed to parse `%s'!" % jsn_file)
                    maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
                    continue
                eventsNumber = int(settings['data'][1])
                fileName = str(settings['data'][3])
                fileSize = int(settings['data'][4])
                lumiSection = int(fileName.split('_')[1].strip('ls'))
                #streamName = str(fileName.split('_')[2].strip('stream'))
                streamName = str(fileName.split('_')[2].split('stream')[1])
                dat_file = os.path.join(rundir, fileName)
                if streamName in _streams_with_scalers:
                    monitor_rates(jsn_file)
                if streamName in _streams_to_postpone:
                    continue
                if streamName in _streams_to_dqm:
                    ## TODO: Use some other temporary directory instead of scratch
                    maybe_move(jsn_file, scratch_rundir)
                    maybe_move(dat_file, scratch_rundir)
                    jsn_file = jsn_file.replace(rundir, scratch_rundir)
                    dat_file = dat_file.replace(rundir, scratch_rundir)
                    args = [dat_file, jsn_file, dqm_rundir_open, dqm_rundir]
                    dqm_pool.apply_async(move_files, args)
                    continue
                if streamName in _streams_to_ecal:
                    ## TODO: Use some other temporary directory instead of scratch
                    maybe_move(jsn_file, scratch_rundir)
                    maybe_move(dat_file, scratch_rundir)
                    jsn_file = jsn_file.replace(rundir, scratch_rundir)
                    dat_file = dat_file.replace(rundir, scratch_rundir)
                    args = [dat_file, jsn_file, ecal_rundir_open, ecal_rundir]
                    ecal_pool.apply_async(move_files, args)
                    continue
                if (run_key == 'TIER0_TRANSFER_OFF' or
                    streamName in (_streams_with_scalers +
                                   _streams_to_ignore)):
                    maybe_move(jsn_file, scratch_rundir)
                    maybe_move(dat_file, scratch_rundir)
                    continue
                if (fileSize > max_tier0_transfer_file_size):
                    logger.warning(
                        "`{0}' too large ({1} > {2})! ".format(
                            dat_file, fileSize, max_tier0_transfer_file_size
                        ) +
                        "Moving it to bad area with the suffix `TooLarge' ..."
                    )
                    maybe_move(jsn_file, new_rundir_bad, suffix='TooLarge')
                    maybe_move(dat_file, new_rundir_bad, suffix='TooLarge')
                    continue
                starttime = int(os.stat(dat_file).st_atime)
                stoptime  = int(os.stat(jsn_file).st_ctime)
                maybe_move(dat_file, new_rundir)
                maybe_move(jsn_file, new_rundir)
                ## Call the actual inject script
                if eventsNumber == 0:
                    number_of_files = 0
                else:
                    number_of_files = 1
                    args_insert = [
                        './insertFile.pl',
                        '--FILENAME'     , fileName,
                        '--FILECOUNTER'  , 0,
                        '--NEVENTS'      , 0,
                        '--FILESIZE'     , 0,
                        '--STARTTIME'    , starttime,
                        '--STOPTIME'     , 0,
                        '--STATUS'       , 'open',
                        '--RUNNUMBER'    , run_number,
                        '--LUMISECTION'  , lumiSection,
                        '--PATHNAME'     , rundir,
                        '--HOSTNAME'     , hostname,
                        '--SETUPLABEL'   , 'Data',
                        '--STREAM'       , streamName,
                        '--INSTANCE'     , 1,
                        '--SAFETY'       , 0,
                        '--APPVERSION'   , appversion,
                        '--APPNAME'      , 'CMSSW',
                        '--TYPE'         , 'streamer',
                        '--CHECKSUM'     , 0,
                        '--CHECKSUMIND'  , 0,
                    ]
                    args_close = [
                        './closeFile.pl',
                        '--FILENAME'    , fileName,
                        '--FILECOUNTER' , 0,
                        '--NEVENTS'     , eventsNumber,
                        '--FILESIZE'    , fileSize,
                        '--STARTTIME'   , starttime,
                        '--STOPTIME'    , stoptime,
                        '--STATUS'      , 'closed',
                        '--RUNNUMBER'   , run_number,
                        '--LUMISECTION' , lumiSection,
                        '--PATHNAME'    , new_rundir,
                        '--HOSTNAME'    , hostname,
                        '--SETUPLABEL'  , 'Data',
                        '--STREAM'      , streamName,
                        '--INSTANCE'    , 1,
                        '--SAFETY'      , 0,
                        '--APPVERSION'  , appversion,
                        '--APPNAME'     , 'CMSSW',
                        '--TYPE'        , 'streamer',
                        '--DEBUGCLOSE'  , 2,
                        '--CHECKSUM'    , 0,
                        '--CHECKSUMIND' , 0,
                    ]
                    inject_file_path = os.path.join(
                        cfg.get('Output', 'inject_base'),
                        '{date}-{hostname}.log'.format(
                            date=datetime.date.today().strftime('%Y%m%d'),
                            hostname=hostname,
                        )
                    )
                    with open(inject_file_path, 'a') as inject_file:
                        line = ' '.join(map(str, args_insert))
                        logger.info(
                            "Appending line `%s' to `%s' ..." % (
                                line, inject_file_path
                            )
                        )
                        inject_file.write(line + '\n')
                        line = ' '.join(map(str, args_close))
                        logger.info(
                            "Appending line `%s' to `%s' ..." % (
                                line, inject_file_path
                            )
                        )
                        inject_file.write(line + '\n')
                try:
                    bookkeeper.fill_number_of_files(
                        cursor, streamName, lumiSection, number_of_files
                    )
                    connection.commit()
                except cx_Oracle.IntegrityError:
                    print ('WARNING: Failed to insert bookkeeping for ' +
                           'run {0}, stream {1}, ls {2}: #files = {3}').format(
                               run_number, streamName, lumiSection,
                               number_of_files
                           )


        ## Move the bad area to new run dir so that we can check for run
        ## completeness
        for fname in glob.glob(os.path.join(rundir_bad, '*.jsn')):
            try:
                jsn = metafile.File(fname)
                if jsn.type == metafile.Type.MacroMerger:
                    dat_path = jsn.path.replace('.jsn', '.dat')
                    maybe_move(jsn.path, new_rundir_bad)
                    maybe_move(dat_path, new_rundir_bad)
            except ValueError:
                logger.warning("Illegal filename `%s'!" % fname)
    connection.close()
## iterate()


#_______________________________________________________________________________
def get_new_path(path, new_base):
    '''
    Given the path to watch, returns the new path under which the files 
    being transferred are moved.
    '''
    head, tail = os.path.split(path)
    return os.path.join(head, new_base)
## get_new_path()


#_______________________________________________________________________________
def mkdir(path):
    logger.info("Making directory `%s' ..." % path)
    os.mkdir(path)
## mkdir()


#_______________________________________________________________________________
def get_rundirs_and_hltkeys(path, new_path):
    
    _run_number_min = cfg.getint('Misc','run_number_min')
    _run_number_max = cfg.getint('Misc','run_number_max')

    rundirs, runs, hltkeymap = [], [], {}
    for rundir in sorted(glob.glob(os.path.join(path, 'run*'))):
        run_number = get_run_number(rundir)
        if run_number < _run_number_min or _run_number_max < run_number:
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
    logger.info('HLT keys: ' + format_hltkeys(hltkeys))
    logger.debug('HLT keys: ' + pprint.pformat(hltkeys))
    return rundirs, hltkeys
## get_rundirs_and_hltkeys()


#_______________________________________________________________________________
def get_run_number(rundir):
    run_token = rundir.split('_')[0]
    return int(os.path.basename(run_token).replace('run', ''))
## get_run_number


#_______________________________________________________________________________
def get_cmssw_version(run_number):

    _old_cmssw_version = cfg.get('Misc','old_cmssw_version')
    current_cmssw_version = _old_cmssw_version
    ## Sort the first_run -> new_cmssw_version map by the first_run
    sorted_rv_pairs = sorted(_first_run_to_new_cmssw_version_map.items(),
                             key=lambda x: x[0])
    for first_run, new_cmssw_version in sorted_rv_pairs:
        if first_run <= run_number:
            current_cmssw_version = new_cmssw_version
        else:
            break
    return current_cmssw_version
## get_cmssw_version()


#_______________________________________________________________________________
def monitor_rates(jsn_file):
    fname = jsn_file + 'data'
    basename = os.path.basename(fname)
    try:
        logger.info('Inserting %s in WBM ...' % basename)
        monitorRates.monitorRates(fname)
    except cx_Oracle.IntegrityError:
        logger.warning('DB record for %s already present!' %  basename)
## monitor_rates


#_______________________________________________________________________________
def mock_move_file_to_dir(src, dst, force_overwrite=False, suffix=None):
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
    logger.info("I would do: mv %s %s" % (src, dst))
## mock_move_file_to_dir()


#_______________________________________________________________________________
def move_file_to_dir(src, dst_dir, force_overwrite=False, suffix=None):
    '''
    Moves the file src to the directory dst_dir. Creates dst_dir if it doesn't exist.
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
    logger.info("Running `mv %s %s' ..." % (src, dst_path))
    try:
        shutil.move(src, dst_path)
    except IOError as error:
        if error.errno == 2 and error.filename == dst_path:
            ## Directory dst_dir doesn't seem to exist. Let's create it.
            logger.info(
                "Failed moving `%s' b/c destination `%s' does not exist." % (
                os.path.basename(src), dst_dir
                )
            )
            mkdir_with_parents(dst_dir)
            logger.info("Retrying `mv %s %s' ..." % (src, dst_path))
            shutil.move(src, dst_path)
        else:
            logger.error(
                "errno: %d, filename: %s, message: %s" % (
                   error.errno, error.filename, error.strerror
                )
            )
            raise error
## move_file_to_dir()

#_______________________________________________________________________________
def move_files(datFile, jsnFile, final_rundir_open, final_rundir):
    try:
        # first move to open area
        maybe_move(datFile, final_rundir_open)
        maybe_move(jsnFile, final_rundir_open)
        # then move to the final area
        maybe_move(os.path.join(final_rundir_open,os.path.basename(datFile)),
                   final_rundir)
        maybe_move(os.path.join(final_rundir_open,os.path.basename(jsnFile)),
                   final_rundir)
    except Exception as e:
        logger.exception(e)
## move_files()


#_______________________________________________________________________________
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


#_______________________________________________________________________________
def log_and_do_not_exec(args, print_output=False):
    ## Make sure all arguments are strings; cast integers.
    args = map(str, args)
    log("I would run:\n  %s" % ' '.join(args))
## log_and_do_not_exec()


#_______________________________________________________________________________
def need_to_retransfer(out):
    for status in _file_status_list_to_retransfer:
        if status.lower() in out.lower():
            return True
    return False
## need_to_retransfer()


#_______________________________________________________________________________
def log(msg, newline=True):
    msg = "%s: %s" % (strftime(), msg)
    if newline:
        logger.info(msg)
    else:
        logger.info(msg)
## log()


#_______________________________________________________________________________
def strftime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
## strftime()


#_______________________________________________________________________________
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


#_______________________________________________________________________________
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


#_______________________________________________________________________________
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


#_______________________________________________________________________________
if __name__ == '__main__':

    main(_path)