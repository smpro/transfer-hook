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

hltkeysscript = "/opt/transferTests/hltKeyFromRunInfo.pl"
injectscript = "/opt/transferTests/injectFileIntoTransferSystem.pl"
#injectscript = "/opt/transferTests/testParams.sh"
## EventDisplay and DQMHistograms should not be transferred
## DQM should be transferred but it's taken out because it causes 
## problems
_streams_to_ignore = ['EventDisplay', 'DQMHistograms', 'DQM']
_run_number_min = 226673
_run_number_max = 300000

def get_runs_and_hltkey(path, hltkeys):
    
    runs = []
    for run in glob.glob(path):
        runNumber = os.path.basename(run).replace('run', '')
        run_number = int(runNumber)
        if run_number < _run_number_min or _run_number_max < run_number:
            continue
        runs.append(run)
        if runNumber not in hltkeys.keys():
     
            args = [hltkeysscript, '-r', runNumber]
            log("I'll run:\n  %s" % ' '.join(args))
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            #print out, err
            if err:
                #print "no hltkey associated with the run", runNumber, ", setting it to UNKOWN"
                hltkeys[runNumber] = "UNKNOWN"
            else:
                hltkeys[runNumber] = out.strip()
    runs.sort()
    return runs


def watch_and_inject(path):
    
    hltkeys = dict()
  
    #put everything in an infinite loop if we want to transfer everything - too much junk for now
    #while True:

    #this gets all the hltkeys, useful only if the watched path is clean - and we actually loop through all the existing runs
    runs_to_transfer = get_runs_and_hltkey(path, hltkeys)
    log('Runs to transfer: ', newline=False) 
    pprint.pprint(runs_to_transfer)
    log('HLT keys: ', newline=False)
    pprint.pprint(hltkeys)
    for run in runs_to_transfer:
        runNumber = os.path.basename(run).replace('run', '')
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

                    args_transfer = [injectscript   ,
                            '--filename'   , fileName,
                            "--path"       , run,
                            "--type"       , "streamer",
                            "--runnumber"  , runNumber,
                            "--lumisection",  str(lumiSection),
                            "--numevents"  , str(eventsNumber),
                            "--appname"    , "CMSSW",
                            "--appversion" , "CMSSW_7_1_9_patch1",
                            "--stream"     , streamName,
                            "--setuplabel" , "Data",
                            "--config"     , "/opt/injectworker/.db.conf",
                            "--destination", "Global",
                            "--filesize"   , str(fileSize),
                            "--hltkey"     , hltkeys[runNumber]]

                    #this is to check the status of the files
                    args_check = [injectscript, '--check'             ,
                            '--filename', fileName                    ,
                            "--config"  , "/opt/injectworker/.db.conf",]
                    args = args_check
                    log("I'll run:\n  %s" % ' '.join(args))
                    p = subprocess.Popen(args, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE)

                    out, err = p.communicate()
                    print out
                    print err
                    if 'File not found in database.' in out:
                        args = args_transfer
                        print 'Ready to transfer', jsn_file
                        print "I'll run:\n", ' '.join(args)
                        p = subprocess.Popen(args, stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
    
                        out, err = p.communicate()
                        print out
                        print err
                    #if "File sucessfully submitted for transfer" in out:
                        #shutil.move(jsn_file,run + "/transferred/" + os.path.basename(jsn_file))
                        #shutil.move(run + fileName, run + "/transferred/" + fileName)
                    #else:
                    #    print "I've encountered some error:\n", out
        
def log(msg, newline=True):
    msg = "%s: %s" % (strftime(), msg)
    if newline:
	print msg
    else:
	print msg,
 
def strftime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

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
