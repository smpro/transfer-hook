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
import errno
import glob 
import json
import logging
import os
import pprint
import shlex
import shutil
import subprocess
import sys
import time
import multiprocessing
from multiprocessing.pool import ThreadPool

import bookkeeper as bookkeeper
import monitorRates as monitorRates
import metafile as metafile
import eor as eor

from optparse import OptionParser
from subprocess import call

from runinfo import RunInfo
from config import Config

__author__     = 'Lavinia Darlea, Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = ['Dirk Hufnagel', 'Guillelmo Gomez-Ceballos']

__licence__    = 'Unknonw'
__version__    = '0.2.3'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'


#logger = logging.getLogger(__name__)
from Logging import getLogger
logger = getLogger()

#from Logging import getLogger
#logger = logging.getLogger()

_dry_run = False
_max_iterations = float("inf")
_max_exceptions = 10
_seconds_to_sleep = 2
_hltkeysscript = '/opt/transferTests/hltKeyFromRunInfo.pl'
_injectscript = '/opt/transferTests/injectFileIntoTransferSystem.pl'
_new_path_base = 'transfer'
_scratch_base = 'scratch'
_dqm_base = '/dqmburam/transfer'  ## Not mounted yet
_ecal_base = '/store/calibarea/global'
##_new_path_base = 'transfer_minidaq'
_streams_to_ignore = ['EventDisplay', 'CalibrationDQM', 'Error']
_streams_to_dqm = ['DQMHistograms', 'DQM', 'DQMCalibration', 'CalibrationDQM']
_streams_to_ecal = ['EcalCalibration']
_streams_with_scalers = ['L1Rates', 'HLTRates']
_streams_to_postpone = []
_run_number_min = 233749 # Begin of CRUZET Feb 2015
_run_number_max = 300000

_old_cmssw_version = 'CMSSW_7_1_9_patch1'
_first_run_to_new_cmssw_version_map = {
    226911: 'CMSSW_7_1_10',
    227163: 'CMSSW_7_1_10_patch1',
    227356: 'CMSSW_7_1_10_patch2',
    228783: 'CMSSW_7_2_1',
    229521: 'CMSSW_7_2_1_patch2',
    229710: 'CMSSW_7_2_1_patch4',
    229831: 'CMSSW_7_2_3',
    }

_file_status_list_to_retransfer = [
    'FILES_TRANS_NEW',
    'FILES_TRANS_COPIED',
    #'FILES_TRANS_CHECKED',
    #'FILES_TRANS_INSERTED',
    ]

## Defualt is False, set this to True if you want to re-transfer.
_renotify = False

#_db_config = '.db.int2r.stomgr_w.cfg.py' # integration
_db_config = '/opt/transfers/.db.rcms.stomgr_w.cfg.py' # production
execfile(_db_config)
_db_sid = db_sid
_db_user = db_user
_db_pwd = db_pwd


_path = '/opt/transfers/mock_directory/mergeMacro'

#______________________________________________________________________________
def main(path):
    '''
    Main entry point to execution.
    '''
    #options, args = parse_args()
    setup()
    caught_exception_count = 0
    iteration = 0
    logger.info('Testing...')

    logger.info('Trying to call eor.main')
    process = multiprocessing.Process(target = eor.main, args = [])
    process.start()
    #process.join()
    logger.info('Finished calling eor.main')

    while True:
        iteration += 1
        if iteration > _max_iterations:
            break
        logger.info('Start iteration {0} of {1} ...'.format(iteration,_max_iterations))
        try:
            iterate(path)
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
    #logging.basicConfig(
    #    level=logging.INFO,
    #    format=r'%(asctime)s %(name)s %(levelname)s %(thread)d: %(message)s',
    #    filename='wai.log'
    #)
    bookkeeper._dry_run = _dry_run
    bookkeeper.setup()
    runinfo = RunInfo('/opt/transfers/.db.omds.runinfo_r.cfg.py')
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
def iterate(path):
    connection = cx_Oracle.connect(_db_user, _db_pwd, _db_sid)
    cursor = connection.cursor()
    new_path = get_new_path(path, _new_path_base)
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
                maybe_move(jsn_file, new_rundir)
                maybe_move(dat_file, new_rundir)
                ## Call the actual inject script
                if eventsNumber == 0:
                    number_of_files = 0
                else:
                    number_of_files = 1
                    args_transfer = [_injectscript,
                            '--filename'   , fileName,
                            "--path"       , new_rundir,
                            "--type"       , "streamer",
                            "--runnumber"  , run_number,
                            "--lumisection", lumiSection,
                            "--numevents"  , eventsNumber,
                            "--appname"    , "CMSSW",
                            "--appversion" , appversion,
                            "--stream"     , streamName,
                            "--setuplabel" , "Data",
                            "--config"     , "/opt/injectworker/.db.conf",
                            "--destination", "Global",
                            "--filesize"   , str(fileSize),
                            "--hltkey"     , hlt_key,]
                    if _renotify:
                        args_transfer.append('--renotify')
                    log_and_maybe_exec(args_transfer, print_output=True)
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
        new_rundir_bad = os.path.join(new_rundir, 'bad')
        for fname in glob.glob(os.path.join(rundir, 'bad', '*.jsn')):
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
def get_new_path(path, new_base=_new_path_base):
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
    logger.info(
        "Inspecting %d dirs in `%s' for runs %s to %s ..." % (
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
def mock_move_file_to_dir(src, dst):
    '''
    Prints a message about how it would move the file src to the directory dst
    if this was for real.
    '''
    ## Append the filename to the destination directory
    dst = os.path.join(dst, os.path.basename(src))
    logger.info("I would do: mv %s %s" % (src, dst))
## mock_move_file_to_dir()


#_______________________________________________________________________________
def move_file_to_dir(src, dst_dir, force_overwrite=False):
    '''
    Moves the file src to the directory dst_dir. Creates dst_dir if it doesn't exist.
    '''
    ## Append the filename to the destination directory
    src_dir , basename = os.path.split(src)
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
