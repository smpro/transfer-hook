#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, sys
from optparse import OptionParser
import shlex, subprocess
from subprocess import call
import glob 
import json
import shutil

hltkeysscript = "/opt/transferTests/hltKeyFromRunInfo.pl"
injectscript = "/opt/transferTests/injectFileIntoTransferSystem.pl"
#injectscript = "/opt/transferTests/testParams.sh"
## EventDisplay and DQMHistograms should not be transferred
## DQM should be transferred but it's taken out because it causes 
## problems
_streams_to_ignore = ['EventDisplay', 'DQMHistograms', 'DQM']
_run_number_min = 226485
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
            print "I'll run:\n", ' '.join(args)
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            #print out, err
            if err:
                #print "no hltkey associated with the run", runNumber, ", setting it to UNKOWN"
                hltkeys[runNumber] = "UNKNOWN"
            else:
                hltkeys[runNumber] = out.strip()
    return runs


def watch_and_inject(path):
    
    hltkeys = dict()
  
    #put everything in an infinite loop if we want to transfer everything - too much junk for now
    #while True:

    #this gets all the hltkeys, useful only if the watched path is clean - and we actually loop through all the existing runs
    runs = get_runs_and_hltkey(path, hltkeys)


    # MWGR4 good runs: 225075, 225080, 225115, 225117, 225119, 225125
    mwgr4runs = [225075, 225080, 225115, 225117, 225119, 225125]
    mwgr5runs1 = [225709, 225713]
    mwgr5runs2 = [225826, 225829, 225832, 225834, 225838, 225843, 
                  225849, 225860, 225861, 225862, 225893, 225896,
                  225904, 225906, 225909, 225910, 225916, 225918, 
                  225919, 225930, 225948, 225949, 225953, 225956, ]
    mwgr7runs = [226485,]
    runs_to_transfer = runs
    print 'Runs to transfer:', runs_to_transfer
    print 'HLT keys:', hltkeys
    for run in runs_to_transfer[:]:
        runNumber = os.path.basename(run).replace('run', '')
        run = "/store/lustre/mergeMacro/run" + runNumber
        # run = "/store/lustre/oldMergeMacro/run" + runNumber
        #try:
        #    os.mkdir(run + "/transferred")
        #except OSError, e:
        #    continue
        
        print "************ Run ", runNumber, " *******************"


        jsns = glob.glob(run + '/*jsn')
        for jsn_file in jsns[:]:
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
                    print "I'll run:\n", ' '.join(args)
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
 

    watch_and_inject(os.path.join(options.path, 'run*'))

