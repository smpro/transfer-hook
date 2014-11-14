#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
TODO:
   * Include lumi bookkeeping. (first prototype done 2014/10/09)
   * Add checksums
   * Move to using the log file as the transfer test instead inject*.pl
   * Query the DB more efficiently similar to ~/smpro/scripts/checkRun.pl
   * Only process each JSON file once. Move both the JSON and data to a new 
     location first. Then inject it in the transfer.
'''

__author__     = 'Lavinia Darlea, Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = ['Dirk Hufnagel', 'Guillelmo Gomez-Ceballos']

__licence__    = 'Unknonw'
__version__    = '0.2.2'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'

import cx_Oracle
import glob 
import json
import os
import pprint
import shlex
import shutil
import subprocess
import sys
import time

import bookkeeper

from optparse import OptionParser
from subprocess import call

_dry_run = False
_max_iterations = 10000
_seconds_to_sleep = 120
_hltkeysscript = "/opt/transferTests/hltKeyFromRunInfo.pl"
_injectscript = "/opt/transferTests/injectFileIntoTransferSystem.pl"
_new_path_base = 'transfer'
#_new_path_base = 'transfer_minidaq'
_streams_to_ignore = ['EventDisplay', 'DQMHistograms', 'DQM', 'CalibrationDQM', 
                      'DQMCalibration', 'Error', 'HLTRates', 'L1Rates']
_run_number_min = 229714
_run_number_max = 300000

_old_cmssw_version = 'CMSSW_7_1_9_patch1'
_first_run_to_new_cmssw_version_map = {
    226911: 'CMSSW_7_1_10',
    227163: 'CMSSW_7_1_10_patch1',
    227356: 'CMSSW_7_1_10_patch2',
    228783: 'CMSSW_7_2_1',
    229521: 'CMSSW_7_2_1_patch2',
    229710: 'CMSSW_7_2_1_patch4',
    }

_file_status_list_to_retransfer = [
    'FILES_TRANS_NEW',
    'FILES_TRANS_COPIED',
    #'FILES_TRANS_CHECKED',
    #'FILES_TRANS_INSERTED',
    ]

# _db_config = '.db.int2r.stomgr_w.cfg.py' # integration
_db_config = '.db.rcms.stomgr_w.cfg.py' # production
execfile(_db_config)
_db_sid = db_sid
_db_user = db_user
_db_pwd = db_pwd

#_______________________________________________________________________________
def main():
    '''
    Main entry point to execution.
    '''
    options, args = parse_args()
    setup()
    for iteration in range(1, _max_iterations + 1):
        print '======================================'
        print '============ ITERATION %d ============' % iteration
        print '======================================'
        iterate(options.path)
        time.sleep(_seconds_to_sleep)
## main()


#_______________________________________________________________________________
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

#_______________________________________________________________________________
def setup():
    global log_and_maybe_exec
    global maybe_move
    bookkeeper._dry_run = _dry_run
    bookkeeper.setup()
    if _dry_run:
        log_and_maybe_exec = log_and_do_not_exec
        maybe_move = mock_move_to_new_rundir
    else:
        log_and_maybe_exec = log_and_exec
        maybe_move = move_to_new_rundir
## setup()

#_______________________________________________________________________________
def iterate(path):
    connection = cx_Oracle.connect(_db_user, _db_pwd, _db_sid)
    cursor = connection.cursor()
    new_path = get_new_path(path)
    rundirs, hltkeys = get_rundirs_and_hltkeys(path)
    for rundir in rundirs:
        new_rundir = os.path.join(new_path, os.path.basename(rundir))
        run_number = int(os.path.basename(rundir).replace('run', ''))
        bookkeeper._run_number = run_number
        appversion = get_cmssw_version(run_number)        
        print "************ Run ", run_number, " *******************"
        jsns = glob.glob(os.path.join(rundir, '*.jsn'))
        jsns.sort()
        log('Processing JSON files: ', newline=False)
        pprint.pprint(jsns)
        for jsn_file in jsns:
            if ("streamError" not in jsn_file and
                'BoLS' not in jsn_file and
                'EoLS' not in jsn_file and
                'EoR' not in jsn_file and
                'index' not in jsn_file):
                settings_textI = open(jsn_file, "r").read()
                settings = json.loads(settings_textI)
                if len(settings['data']) < 5:
                    continue
                eventsNumber = int(settings['data'][1])
                fileName = str(settings['data'][3])
                fileSize = int(settings['data'][4])
                lumiSection = int(fileName.split('_')[1].strip('ls'))
                #streamName = str(fileName.split('_')[2].strip('stream'))
                streamName = str(fileName.split('_')[2].split('stream')[1])
                if streamName in _streams_to_ignore:
                    continue
                maybe_move(jsn_file, new_rundir)
                maybe_move(os.path.join(rundir, fileName), new_rundir)
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
                            "--hltkey"     , hltkeys[run_number]]
                    log_and_maybe_exec(args_transfer, print_output=True)
                bookkeeper.fill_number_of_files(cursor, streamName,
                                                lumiSection, number_of_files)
                connection.commit()
    connection.close()
## iterate()


#_______________________________________________________________________________
def get_new_path(path):
    '''
    Given the path to watch, returns the new path under which the files 
    being transferred are moved.
    '''
    head, tail = os.path.split(path)
    return os.path.join(head, _new_path_base)
## get_new_path()


#_______________________________________________________________________________
def get_rundirs_and_hltkeys(path):
    rundirs, hltkeys = [], {}
    for rundir in sorted(glob.glob(os.path.join(path, 'run*'))):
        run_number = get_run_number(rundir)
        if run_number < _run_number_min or _run_number_max < run_number:
            continue
        rundirs.append(rundir)
        if run_number not in hltkeys.keys():
            args = [_hltkeysscript, '-r', str(run_number)]
            out, err = log_and_exec(args)
            if err:
                hltkeys[run_number] = "UNKNOWN"
            else:
                hltkeys[run_number] = out.strip()
    rundirs.sort()
    log('Run directories to transfer: ', newline=False)
    pprint.pprint(rundirs)
    log('HLT keys: ', newline=False)
    pprint.pprint(hltkeys)
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
def mock_move_to_new_rundir(src, dst):
    '''
    Prints a message about how it would move the file src to the directory dst
    if this was for real.
    '''
    ## Append the filename to the destination directory
    dst = os.path.join(dst, os.path.basename(src))
    print "I would do: mv %s %s" % (src, dst)
## mock_move_to_new_rundir()


#_______________________________________________________________________________
def move_to_new_rundir(src, dst):
    '''
    Moves the file src to the directory dst. Creates dst if it doesn't exist.
    '''
    ## Append the filename to the destination directory
    full_dst = os.path.join(dst, os.path.basename(src))
    print "I'll do: mv %s %s" % (src, full_dst)
    try:
        shutil.move(src, full_dst)
    except IOError as error:
        if error.errno == 2 and error.filename == full_dst:
            ## Directory dst doesn't seem to exist. Let's create it.
            print "Failed because destination does not exist."
            print "Creating `%s'." % dst
            os.mkdir(dst)
            print "Retrying: mv %s %s" % (src, full_dst)
            shutil.move(src, full_dst)
        else:
            raise error
## move_to_new_rundir()


#_______________________________________________________________________________
def log_and_exec(args, print_output=False):
    ## Make sure all arguments are strings; cast integers.
    args = map(str, args)
    log("I'll run:\n  %s" % ' '.join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if print_output:
        print out
        print err
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
        print msg
    else:
        print msg,
## log()


#_______________________________________________________________________________
def strftime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
## strftime()


#_______________________________________________________________________________
if __name__ == '__main__':
    main()
