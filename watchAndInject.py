#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
TODO:
   * Include lumi bookkeeping.
   * Add checksums
   * Move to using the log file as the transfer test instead inject*.pl
   * Query the DB more efficiently similar to ~/smpro/scripts/checkRun.pl
   * Only process each JSON file once. Move both the JSON and data to a new 
     location first. Then inject it in the transfer.
## new CMSSW version: 7_1_10
'''
__author__     = 'Lavinia Darlea, Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = ['Dirk Hufnagel', 'Guillelmo Gomez-Ceballos']

__licence__    = 'Unknonw'
__version__    = '0.2.1'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'

import os
import sys
from optparse import OptionParser
import shlex, subprocess
from subprocess import call
import glob 
import json
import shutil
import pprint
import time

_hltkeysscript = "/opt/transferTests/hltKeyFromRunInfo.pl"
_injectscript = "/opt/transferTests/injectFileIntoTransferSystem.pl"
_streams_to_ignore = ['EventDisplay', 'DQMHistograms', 'DQM', 'CalibrationDQM', 
                      'DQMCalibration']
_run_number_min = 226496
_run_number_max = 300000
_old_cmssw_version = 'CMSSW_7_1_9_patch1'
_new_cmssw_version = 'CMSSW_7_1_10'
_first_run_with_new_cmssw_version = 226911
_file_status_list_to_retransfer = [
    'FILES_TRANS_NEW',
    'FILES_TRANS_COPIED',
    ]

def get_runs_and_hltkey(path, hltkeys):
    runs = []
    for run in glob.glob(path):
        runNumber = os.path.basename(run).replace('run', '')
        run_number = int(runNumber)
        if run_number < _run_number_min or _run_number_max < run_number:
            continue
        runs.append(run)
        if runNumber not in hltkeys.keys():
            args = [_hltkeysscript, '-r', runNumber]
            out, err = log_and_exec(args)
            if err:
                hltkeys[runNumber] = "UNKNOWN"
            else:
                hltkeys[runNumber] = out.strip()
    runs.sort()
    return runs


def watch_and_inject(path):
    hltkeys = dict()
    runs_to_transfer = get_runs_and_hltkey(path, hltkeys)
    log('Runs to transfer: ', newline=False) 
    pprint.pprint(runs_to_transfer)
    log('HLT keys: ', newline=False)
    pprint.pprint(hltkeys)
    for run in runs_to_transfer:
        runNumber = os.path.basename(run).replace('run', '')
        if _first_run_with_new_cmssw_version <= int(runNumber):
            appversion = _new_cmssw_version
        else:
            appversion = _old_cmssw_version
        run = "/store/lustre/mergeMacro/run" + runNumber
        # run = "/store/lustre/oldMergeMacro/run" + runNumber
        #try:
        #    os.mkdir(run + "/transferred")
        #except OSError, e:
        #    continue
        
        print "************ Run ", runNumber, " *******************"


        jsns = glob.glob(run + '/*jsn')
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

                #call the actual inject script
                if eventsNumber != 0:


                    #this is to check the status of the files
                    args_check = [_injectscript, '--check'                   ,
                            '--filename', fileName                    ,
                            "--config"  , "/opt/injectworker/.db.conf",]
                    args_transfer = [_injectscript,
                            '--filename'   , fileName,
                            "--path"       , run,
                            "--type"       , "streamer",
                            "--runnumber"  , runNumber,
                            "--lumisection", str(lumiSection),
                            "--numevents"  , str(eventsNumber),
                            "--appname"    , "CMSSW",
                            "--appversion" , appversion,
                            "--stream"     , streamName,
                            "--setuplabel" , "Data",
                            "--config"     , "/opt/injectworker/.db.conf",
                            "--destination", "Global",
                            "--filesize"   , str(fileSize),
                            "--hltkey"     , hltkeys[runNumber]]
                    args_renotify = args_transfer[:] + ["--renotify"]
                    out, err = log_and_exec(args_check, print_output=True)
                    if 'File not found in database.' in out:
                        print 'Ready to transfer', jsn_file
                        log_and_exec(args_transfer, print_output=True)
                    elif need_to_retransfer(out):
                        print 'Ready to re-transfer', jsn_file
                        log_and_exec(args_renotify, print_output=True)
                    #if "File sucessfully submitted for transfer" in out:
                        #shutil.move(jsn_file,run + "/transferred/" + os.path.basename(jsn_file))
                        #shutil.move(run + fileName, run + "/transferred/" + fileName)
                    #else:
                    #    print "I've encountered some error:\n", out

def log_and_exec(args, print_output=False):        
    log("I'll run:\n  %s" % ' '.join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if print_output:
        print out
        print err
    return out, err

def need_to_retransfer(out):
    for status in _file_status_list_to_retransfer:
        if status.lower() in out.lower():
            return True
    return False

def log(msg, newline=True):
    msg = "%s: %s" % (strftime(), msg)
    if newline:
	print msg
    else:
	print msg,
 
def strftime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [-h|--help] [-p|--path]")
    parser.add_option("-p", "--path",
                      action="store", dest="path",
                      help="path to watch for files to be transferred")

    options, args = parser.parse_args()

    if len(args) != 0:
        parser.error("You specified an invalid option - please use -h to review the allowed options")


    if (options.path == None):
        parser.error('Please provide the path to watch')
 
    for iteration in range(1000):
        print '======================================'
        print '============ ITERATION %d ============' % iteration
        print '======================================'
        watch_and_inject(os.path.join(options.path, 'run*'))
        time.sleep(60)
