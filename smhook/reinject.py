#!/usr/bin/env python                                                                                                                                                                
import os
import glob
import json
from smhook.runinfo import RunInfo
import sys
import zlib

import os.path, time
from datetime import datetime, timedelta, date


runinfo = RunInfo('/opt/python/smhook/config/.db.omds.runinfo_r.cfg.py')

def main():
    findmissingfiles('/store/lustre/transfer/','/store/lustre/mergeMacro/')
    findmissingfiles('/store/lustre/transfer_minidaq/','/store/lustre/mergeMiniDAQMacro/')
    
def findmissingfiles(watchpath,mergeMacroPath):

    print 'Looking at',watchpath,'...'

    full_run_list = sorted(glob.glob(os.path.join(watchpath, 'run*')), reverse=True)

    for nf in range(1, min(len(full_run_list),50)):
        #print 'checking run', full_run_list[nf], '...'
        rundir = full_run_list[nf]

        m_time_stamp = int(os.stat(rundir).st_mtime)
        m_utc_date_time = datetime.utcfromtimestamp(m_time_stamp)

        if (datetime.utcnow() - m_utc_date_time) > timedelta(hours=48):
            continue

        number = str(os.path.basename(rundir).replace('run', ''))

        command1 = "export PYTHONPATH=/opt/python/; python checkRun.py -r "+str(number)+" -sm P5_INJECTED >& /tmp/full_list_"+str(number)+".txt"
        command2 = "cat /tmp/full_list_"+str(number)+".txt | grep run | awk -F\":       \" '{ print $2 }' >& /tmp/missing_"+str(number)+".txt"
    
        os.system(command1)
        os.system(command2)

        ## some crazy scenerio where there is only one empty line printed in the file
        if os.path.getsize('/tmp/missing_'+number+'.txt') == 1:
            continue

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

            print 'mv '+dat_file+' '+mergeMacroPath+'/run'+str(number)+'/stream'+streamName+'/data/'+dat_file_strip
            print 'mv '+jsn_file+' '+mergeMacroPath+'/run'+str(number)+'/stream'+streamName+'/jsns/'+jsn_file_strip

        i=i+1
    

if __name__ == '__main__':
    print 'Running the re-inject script for cdaq and minidaq'
    main()

