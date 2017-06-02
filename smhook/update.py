#!/usr/bin/env python                                                                                                                                                                
import os
import glob
import json
from smhook.runinfo import RunInfo
import sys
import zlib
import subprocess

import os.path, time
from datetime import datetime, timedelta, date


runinfo = RunInfo('/opt/python/smhook/config/.db.omds.runinfo_r.cfg.py')

def main():
    updatemissingfiles('/store/lustre/transfer/','/store/lustre/mergeMacro/')
    updatemissingfiles('/store/lustre/transfer_minidaq/','/store/lustre/mergeMiniDAQMacro/')
    
def updatemissingfiles(watchpath,mergeMacroPath):

    #print 'Looking at',watchpath,'...'

    full_run_list = sorted(glob.glob(os.path.join(watchpath, 'run*')), reverse=True)
    
    for nf in range(1, min(len(full_run_list),150)):

        rundir = full_run_list[nf]

        m_time_stamp = int(os.stat(rundir).st_mtime)
        m_utc_date_time = datetime.utcfromtimestamp(m_time_stamp)

        if (datetime.utcnow() - m_utc_date_time) > timedelta(hours=48):
            continue

        number = str(os.path.basename(rundir).replace('run', ''))

        #print 'checking run', full_run_list[nf], '...'

        command1 = "export PYTHONPATH=/opt/python/; python checkRun.py -r "+str(number)+" -sm TRANSFERRED >& /tmp/full_list2_"+str(number)+".txt"
        command2 = "cat /tmp/full_list2_"+str(number)+".txt | grep run | awk -F\":       \" '{ print $2 }' >& /tmp/missing2_"+str(number)+".txt"

        os.system(command1)
        os.system(command2)

        ## some crazy scenerio where there is only one empty line printed in the file
        if os.path.getsize('/tmp/missing2_'+number+'.txt') == 1:
            continue

        path = watchpath+"/run"+number+"/"
        
        i=0

        for line in open('/tmp/missing2_'+number+'.txt','r'):
            line = line.rstrip('\n')
            # protect against empty lines
            if line == "" : continue

            dat_file_strip = line
            
            command3 = "export PYTHONPATH=/opt/python/; python checkRun.py -f "+str(dat_file_strip)+ " -q True >& /tmp/quality_file.txt"
            command4 = "cat /tmp/quality_file.txt | grep 'not filled' | awk -F \" \" '{print $12}'"            

            os.system(command3)
            p = subprocess.Popen(command4, shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            file_update, error = p.communicate()

            if file_update.rstrip('\n') is not '':
                command5 = "export PYTHONPATH=/opt/python/; python checkRun.py -f "+str(file_update).rstrip('\n')+ " -uq True"
                print command5
                os.system(command5)
                
        i=i+1
    

if __name__ == '__main__':
    #print 'Running the re-inject script for cdaq and minidaq'
    main()

