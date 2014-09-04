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

def get_runs_and_hltkey(path, hltkeys):
    
    runs = glob.glob(path)
    for run in runs:
        runNumber = os.path.basename(run).strip('run')
        if runNumber not in hltkeys.keys():
            p = subprocess.Popen([hltkeysscript, '-r', runNumber],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
    #runs = get_runs_and_hltkey(path, hltkeys)


    #print hltkeys
    #for run in runs:
    #for run in ["/store/lustre/mergeMacro/run224074", "/store/lustre/mergeMacro/run224080", "/store/lustre/mergeMacro/run224148", "/store/lustre/mergeMacro/run224187", "/store/lustre/mergeMacro/run224230", "/store/lustre/mergeMacro/run224236", "/store/lustre/mergeMacro/run224238", "/store/lustre/mergeMacro/run224241", "/store/lustre/mergeMacro/run224244", "/store/lustre/mergeMacro/run224259", "/store/lustre/mergeMacro/run224273", "/store/lustre/mergeMacro/run224286", "/store/lustre/mergeMacro/run224307", "/store/lustre/mergeMacro/run224324", "/store/lustre/mergeMacro/run224326", "/store/lustre/mergeMacro/run224328", "/store/lustre/mergeMacro/run224340", "/store/lustre/mergeMacro/run224352", "/store/lustre/mergeMacro/run224359", "/store/lustre/mergeMacro/run224371", "/store/lustre/mergeMacro/run224373", "/store/lustre/mergeMacro/run224374", "/store/lustre/mergeMacro/run224375", "/store/lustre/mergeMacro/run224376", "/store/lustre/mergeMacro/run224379", "/store/lustre/mergeMacro/run224380", "/store/lustre/mergeMacro/run224393", "/store/lustre/mergeMacro/run224397", "/store/lustre/mergeMacro/run224409", "/store/lustre/mergeMacro/run224412", "/store/lustre/mergeMacro/run224413", "/store/lustre/mergeMacro/run224417", "/store/lustre/mergeMacro/run224440", "/store/lustre/mergeMacro/run224457", "/store/lustre/mergeMacro/run224471", "/store/lustre/mergeMacro/run224481", "/store/lustre/mergeMacro/run224482", "/store/lustre/mergeMacro/run224483", "/store/lustre/mergeMacro/run224484", "/store/lustre/mergeMacro/run224497", "/store/lustre/mergeMacro/run224499", "/store/lustre/mergeMacro/run224502", "/store/lustre/mergeMacro/run224506", "/store/lustre/mergeMacro/run224512"]: 

    # MWGR4 good runs: 225075, 225080, 225115, 225117, 225119, 225125
    mwgr4runs = [225075, 225080, 225115, 225117, 225119, 225125]
    mwgr5runs1 = [225709, 225713]
    mwgr5runs2 = [225826, 225829, 225832, 225834, 225838, 225843, 
                  225849, 225860, 225861, 225862, 225893, 225896,
                  225904, 225906, 225909, 225910, 225916, 225918, 
                  225919, 225930, 225948, 225949, 225953, 225956, ]
    runs_to_transfer = mwgr5runs2
    for runNumber in runs_to_transfer[:]:
        runNumber = '%d' % runNumber
        run = "/store/lustre/mergeMacro/run" + runNumber
        #try:
        #    os.mkdir(run + "/transferred")
        #except OSError, e:
        #    continue
        
        print "************ Run ", runNumber, " *******************"

        args = [hltkeysscript, '-r', runNumber]
        print "I'll run:\n", ' '.join(args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if err:
            hltkeys[runNumber] = "UNKNOWN"
        else:
            hltkeys[runNumber] = out.strip()

        jsns = glob.glob(run + '/*jsn')
        for jsn_file in jsns:
            if "streamError" not in  jsn_file:
                settings_textI = open(jsn_file, "r").read()
                settings = json.loads(settings_textI)
                eventsNumber = int(settings['data'][1])
                fileName = str(settings['data'][3])
                fileSize = int(settings['data'][4])
                lumiSection = int(fileName.split('_')[1].strip('ls'))
                #streamName = str(fileName.split('_')[2].strip('stream'))
                streamName = str(fileName.split('_')[2].split('stream')[1])

                #call the actual inject script
                if eventsNumber != 0:

                    #prepare for GR4 --> CMSSW version updated
                    args_transfer = [injectscript   ,
                            '--filename'   , fileName,
                            "--path"       , run,
                            "--type"       , "streamer",
                            "--runnumber"  , runNumber,
                            "--lumisection",  str(lumiSection),
                            "--numevents"  , str(eventsNumber),
                            "--appname"    , "CMSSW",
                            "--appversion" , "CMSSW_7_1_6",
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
                    args = args_transfer
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
 

    watch_and_inject(options.path + "/run*")

