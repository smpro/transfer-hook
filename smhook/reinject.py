#!/usr/bin/env python                                                                                                                                                                
import os
import glob
import json
from smhook.runinfo import RunInfo
import sys
import zlib
import argparse

import os.path, time
from datetime import datetime, timedelta, date


runinfo = RunInfo('/opt/python/smhook/config/.db.omds.runinfo_r.cfg.py')

def main():

    parser = argparse.ArgumentParser(description='Get the commands to reinject files which missed transfer or runs that were taken with the Tier0_off flag')
    parser.add_argument("-r", "--runnumber" , dest="runnumber"    , help="run number, use this if you want to transfer a run that was taken with the Tier0_off flag set", type = int, action = 'store')
    parser.add_argument("-t", "--tier0off",   dest="tier0off"     , help="use the script to get the commands for injecting a run taken with the Tier0_off flag set. These runs cannot be reinjected via the production transfer system", action = 'store_true')
    parser.add_argument("-p", "--production", dest="production"   , help="use the production system rather than the retransfers dedicated node", action = 'store_true')
    parser.add_argument("-c", "--checksum",   dest="checksum"     , help="wether the checksum should be computed before attempting the injection. Only valid when reinjecting files taken within runs that were supposed to go to Tier0. Do not use it when large bunches of files need to be reinjected, checksum computation is resource consuming and heavy on the file system", action = 'store_true')
    
    args, unknown = parser.parse_known_args()
    if unknown:
        print "There is a parameter I do not understand, please refer to the usage, as follows"
        parser.print_help()
        sys.exit(2)

    if not args.tier0off:
        if args.production:
            findmissingfiles('/store/lustre/transfer/','/store/lustre/mergeMacro/', validateChecksum = args.checksum)
            findmissingfiles('/store/lustre/transfer_minidaq/','/store/lustre/mergeMiniDAQMacro/', validateChecksum = args.checksum)
        else:
            findmissingfiles('/store/lustre/transfer/','/store/lustre/retransfer/', validateChecksum = args.checksum, createHierarchy = True)
            findmissingfiles('/store/lustre/transfer_minidaq/','/store/lustre/retransfer/', validateChecksum = args.checksum, createHierarchy = True)
    else:
        if not args.runnumber:
            parser.print_help()
            raise parser.error("You want to transfer a run taken with the Tier0_OFF flag set. Please provide the run number")
        if args.production:
            parser.print_help()
            raise parser.error("The runs taken with Tier0Off flag cannot be reinjected via the production transfer system")       
        if args.checksum:
            parser.print_help()
            raise parser.error("The checksum verification is not available for files that belong to runs taken with the Tier0Off flag")
 
        transferTier0OffRun(args.runnumber)

def transferTier0OffRun (runnumber):
    sourceFolder = '/store/lustre/scratch/run' + str(runnumber) + '/'
    destFolder   = '/store/lustre/retransfer'

    try:
        files = os.listdir(sourceFolder)
    except OSError:
        print 'Looks like there is no', sourceFolder
        sys.exit(0)

    dataFiles = [file for file in files if file.endswith('.dat') and 'DQM' not in file and 'EoR' not in file]
    EoRFiles = [file for file in files if file.endswith('.jsn') and 'EoR' in file]
    #jsnFiles  = [file for file in files if file.endswith('.jsn') and 'DQM' not in file]
    streams = list(set([file.split('_')[2] for file in dataFiles]))
    for stream in streams:
        print 'mkdir -p ' + destFolder + '/run'+str(runnumber)+'/' + stream +'/{data,jsns}'
    for dataFile in dataFiles:
        streamName = dataFile.split('_')[2]
        print 'mv ' + sourceFolder + dataFile + ' ' + destFolder + '/run' + str(runnumber) + streamName + '/data/' + dataFile
        print 'mv ' + sourceFolder + dataFile.replace('.dat', '.jsn') + ' ' + destFolder + '/run' + str(runnumber) + streamName + '/jsns/' + dataFile.replace('.dat', '.jsn')
    for EoRfile in EoRFiles:
        print 'mv ' + sourceFolder + EoRFile + ' ' + destFolder + '/run' + str(runnumber) + EoRFile
    
def findmissingfiles(watchpath,mergeMacroPath, validateChecksum, createHierarchy = False):

    #print 'Looking at',watchpath,'...'

    full_run_list = sorted(glob.glob(os.path.join(watchpath, 'run*')), reverse=True)

    for nf in range(1, min(len(full_run_list),50)):
        #print 'checking run', full_run_list[nf], '...'
        rundir = full_run_list[nf]

        m_time_stamp = int(os.stat(rundir).st_mtime)
        m_utc_date_time = datetime.utcfromtimestamp(m_time_stamp)

        if (datetime.utcnow() - m_utc_date_time) > timedelta(hours=48):
            continue

        number = str(os.path.basename(rundir).replace('run', ''))

        command1 = "export PYTHONPATH=/opt/python/; python /opt/python/smhook/checkRun.py -r "+str(number)+" -sm P5_INJECTED >& /tmp/full_list_"+str(number)+".txt"
        command2 = "cat /tmp/full_list_"+str(number)+".txt | grep run | awk -F\":       \" '{ print $2 }' >& /tmp/missing_"+str(number)+".txt"
    
        os.system(command1)
        os.system(command2)

        ## some crazy scenerio where there is only one empty line printed in the file
        if os.path.getsize('/tmp/missing_'+number+'.txt') == 1:
            continue
        
        if createHierarchy:
            try:
                streams = list(set([l.split('_')[2] for l in open('/tmp/missing_'+number+'.txt','r') if l.strip()]))
            except IndexError:
                continue 
            for stream in streams:
                print 'mkdir -p ' + mergeMacroPath + '/run'+str(number)+'/' + stream +'/{data,jsns}/'

        path = watchpath+"/run"+number+"/"
        
        i=0
        for line in open('/tmp/missing_'+number+'.txt','r'):
            line = line.rstrip('\n')
            # protect against empty lines
            if line == "" : continue
            dat_file_strip = line
            jsn_file_strip = line.strip('dat')+'jsn'
            dat_file = path+line
            jsn_file = path+line.strip('dat')+'jsn'

            settings_textI = open(jsn_file, "r").read()

            settings = json.loads(settings_textI)
            eventsNumber = int(settings['data'][1])
            fileName = str(settings['data'][3])
            lumiSection = int(fileName.split('_')[1].strip('ls'))
            runNum = int(fileName.split('_')[0].strip('run'))
            streamName = str(fileName.split('_')[2].split('stream')[1])
            appversion = str(runinfo.get_cmssw_version(runNum))
            hltkey= str(runinfo.get_hlt_key(runNum))

            ## get checksum from the json
            checksum_int = int(settings['data'][5])
            if validateChecksum:

                ## calculate local checksum
                checkSumIni=1
                with open(dat_file, 'r') as fsrc:
                    length=16*1024
                    while 1:
                        buf = fsrc.read(length)
                        if not buf:
                            break
                        checkSumIni=zlib.adler32(buf,checkSumIni)        
                checkSumIni = checkSumIni & 0xffffffff

                ## if they are not equal, then continue
                if int(checksum_int) != int(checkSumIni):
                    print fileName, 'had mismatched checksum, expected (from jsn)', checksum_int, 'calculated locally', checkSumIni
                    continue
        
                #print dat_file

                print 'mv ' + dat_file + ' ' + mergeMacroPath+'/run' + str(number)+'/stream'+streamName+'/data/'+dat_file_strip
                print 'mv ' + jsn_file + ' ' + mergeMacroPath+'/run' + str(number)+'/stream'+streamName+'/jsns/'+jsn_file_strip
            else:
                print 'mv ' + dat_file + ' ' + mergeMacroPath+'/run' + str(number)+'/stream'+streamName+'/data/'+dat_file_strip
                print 'mv ' + jsn_file + ' ' + mergeMacroPath+'/run' + str(number)+'/stream'+streamName+'/jsns/'+jsn_file_strip

        i=i+1
    

if __name__ == '__main__':
    #print 'Running the re-inject script for cdaq and minidaq'
    main()

