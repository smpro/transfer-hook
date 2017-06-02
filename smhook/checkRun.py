#!/bin/env python 

'''
These are operational scripts that will help us
understand the status of the file and quality
'''

import os,sys,socket,argparse
import shutil
import time,datetime
import cx_Oracle
import json
import logging

import smhook.config
import smhook.databaseAgent as databaseAgent
import smhook.copyWorker as copyWorker
import smhook.injectWorker as injectWorker
import smhook.fileQualityControl as fileQualityControl

logger = logging.getLogger(__name__)

status_flags = {
    1: 'P5_INJECTED',
    2: 'TRANSFERRED',
    3: 'T0_CHECKED' ,
    4: 'T0_REPACKED'
}

reverse_status_flags = {v: k for k, v in status_flags.iteritems()}

def checkRunStatus(status, runnumber):
    query  = "SELECT FILE_ID FROM CMS_STOMGR.FILE_TRANSFER_STATUS "+\
             "WHERE STATUS_FLAG={0} AND RUNNUMBER={1}"
    query  = query.format(str(status),str(runnumber))
    result = databaseAgent.runQuery('file_status', query, fetch_output=True)
    return len(result)

def checkStatus(file_id):
    query = "SELECT STATUS_FLAG FROM CMS_STOMGR.FILE_TRANSFER_STATUS "+\
            " WHERE FILE_ID={0}"
    query=query.format(str(file_id))
    result = databaseAgent.runQuery('file_status', query, fetch_output=True)
    return result[0][0]

def findstatus_T0(status):
    query = "SELECT P5_FILEID FROM CMS_T0DATASVC_PROD.FILE_TRANSFER_STATUS_OFFLINE WHERE "+status+"_RETRIEVE=1"
    result_fileId=databaseAgent.runQuery('file_status_T0', query, fetch_output=True)
    return result_fileId

def findstatus(status,runnumber):
    query = "SELECT FILE_ID FROM CMS_STOMGR.FILE_TRANSFER_STATUS WHERE STATUS_FLAG={0} AND RUNNUMBER={1}"
    query = query.format(reverse_status_flags[status],str(runnumber))
    result_fileId=databaseAgent.runQuery('file_status', query, fetch_output=True)
    return result_fileId

def findStatusNotDeleted(status, runnumber):
    query = "SELECT FILE_ID FROM CMS_STOMGR.FILE_TRANSFER_STATUS WHERE DELETED_FLAG=0 AND STATUS_FLAG={0} AND RUNNUMBER={1}"
    query = query.format(reverse_status_flags[status], str(runnumber))
    result_fileId=databaseAgent.runQuery('file_status', query, fetch_output=True)
    return result_fileId


def checkStatus_T0(file_id):
    query = "SELECT CHECKED_RETRIEVE FROM CMS_T0DATASVC_PROD.FILE_TRANSFER_STATUS_OFFLINE "+\
            " WHERE P5_FILEID={0}" 
    query2 = "SELECT REPACKED_RETRIEVE FROM CMS_T0DATASVC_PROD.FILE_TRANSFER_STATUS_OFFLINE "+\
            " WHERE P5_FILEID={0}" 
    query=query.format(str(file_id))  
    query2=query2.format(str(file_id))  
    result=databaseAgent.runQuery('file_status_T0', query, fetch_output=True)
    result2=databaseAgent.runQuery('file_status_T0', query2, fetch_output=True)
    
    if len(result)>0 and len(result) > 0:
        return result[0][0],result2[0][0]
    else:
        return False, False

def findInconsistency(origstatus):        

    query = "SELECT P5_FILEID FROM CMS_T0DATASVC_PROD.FILE_TRANSFER_STATUS_OFFLINE "+\
            " WHERE T0_"+origstatus+"_TIME IS NOT NULL AND "+origstatus+"_RETRIEVE IS NULL"
    file_ids=databaseAgent.runQuery('file_status_T0', query, fetch_output=True)

    for l in range(0,len(file_ids)):
        status = checkStatus(file_ids[l][0])
        if status < reverse_status_flags["T0_"+origstatus]:
            query_statusUpdate_SM = "BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
                                    "SET STATUS_FLAG={0} "+\
                                    "WHERE FILE_ID={1}; COMMIT; END;"
            query_statusUpdate_SM  = query_statusUpdate_SM.format(reverse_status_flags["T0_"+origstatus],file_ids[l][0])
            result_statusUpdate_SM = databaseAgent.runQuery('file_status', query_statusUpdate_SM, fetch_output=False)
            status2 = checkStatus(file_ids[l][0])

def updateStatus(status, file_id, dryrun=False):    
    query = "BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS " +\
            "SET STATUS_FLAG={0} " +\
            "WHERE FILE_ID={1}; COMMIT; END;"
    query=query.format(reverse_status_flags[status],file_id)
    if dryrun:
        logger.info("Running in try mode, the query that would have been executed is {0}".format(query))
        return True
    else:
        result = databaseAgent.runQuery('file_status',query,fetch_output=True)
        return result

def checkDeletedStatus(runnumber):
    query = "SELECT FILE_ID FROM CMS_STOMGR.FILE_TRANSFER_STATUS "+\
            "WHERE DELETED_FLAG=1 AND RUNNUMBER={0}"
    query  = query.format(str(runnumber))
    result = databaseAgent.runQuery('file_status', query, fetch_output=True)
    return len(result)

def checkDeletedStatus_perfile(fileid):
    query = "SELECT DELETED_FLAG FROM CMS_STOMGR.FILE_TRANSFER_STATUS "+\
            "WHERE FILE_ID={0}"
    query  = query.format(fileid)
    result = databaseAgent.runQuery('file_status', query, fetch_output=True)
    return result

def updateDeleted(file_id, dryrun=False):
    query = "BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS " +\
            "SET DELETED_FLAG=1 "+\
            "WHERE FILE_ID={0}; COMMIT; END;"
    query=query.format(file_id)
    if dryrun:
        logger.info("Running in try mode, the query that would have been executed is {0}".format(query))
        return True
    else:
        result = databaseAgent.runQuery('file_status',query,fetch_output=False)
        return result

def checkFileQuality(filename):
    query = "SELECT FILE_SIZE, EVENTS_BUILT, EVENTS_ACCEPTED, EVENTS_LOST, IS_GOOD_LS FROM CMS_STOMGR.FILE_QUALITY_CONTROL "+\
            " WHERE FILENAME={0}"
    query=query.format("'"+filename+"'")
    result = databaseAgent.runQuery('file_status', query, fetch_output=True)
    print result
    return result

def updateFileQuality(fileName,dryrun=False,wantInfo=False):
    jsn_file = fileName.replace('.dat','.jsn')
    tokens = jsn_file.split('_')
    if len(tokens) !=4:
        return None
    run_token, ls_token, stream_token, sm_token = tokens
    run_len, ls_len, stream_len, sm_len = map(len, tokens)
    if (run_token   [:len('run')   ] != 'run'         or
        ls_token    [:len('ls')    ] != 'ls'          or
        stream_token[:len('stream')] != 'stream'      or
        sm_token                     != 'StorageManager'):
        print run_token, ls_token, stream_token, sm_token

    run_number  = int(run_token[len('run')   :])
    lumiSection = int(ls_token [len('ls')    :])
    streamName  = stream_token [len('stream'):]

    settings_textI = open("/store/lustre/transfer/run"+str(run_number)+"/"+jsn_file, "r").read()
    settings = json.loads(settings_textI)
    fileSize = int(settings['data'][4])
    inputEvents = int(settings['data'][0])
    eventsNumber = int(settings['data'][1])
    errorEvents = int(settings['data'][2]) # BU/FU crash                                                                                                                                                                 
    events_built=inputEvents+errorEvents
    events_lost_checksum=0
    events_lost_cmssw=0
    events_lost_crash=errorEvents
    events_lost_oversized=0
    is_good_ls=True

    if dryrun:
        logger.info("Running in drymode, would have executed the fileQuality Update for file {0}".format(fileName))
        if wantInfo:
            return fileName, run_number, lumiSection, streamName, fileSize, eventsNumber, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls
           #return fileName, run_number, lumiSection, streamName, fileSize, events_built, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls
    else:
        fileQualityControl.fileQualityControl(fileName, run_number, lumiSection, streamName, fileSize, eventsNumber, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls) 
       #fileQualityControl.fileQualityControl(fileName, run_number, lumiSection, streamName, fileSize, events_built, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls) 

def copyFileToT0(dat_file,file_id,dryrun=False):
    fileName, run_number, lumiSection, streamName, fileSize, events_built, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls = updateFileQuality(dat_file,True,True)
    esServerUrl = ""
    esIndexName = ""
    monitor_fqc = False
    new_file_path = "/store/lustre/transfer/run"+str(run_number)+"/"+dat_file
    new_rundir_bad = "/store/lustre/transfer/run"+str(run_number)+"/bad"
    _eos_destination = "/store/t0streamer/"
    setup_label = "Data"

    jsn_file = dat_file.replace('.dat','.jsn')
    settings_textI = open("/store/lustre/transfer/run"+str(run_number)+"/"+jsn_file, "r").read()
    settings = json.loads(settings_textI)
    checksum_int = int(settings['data'][5]) 
    checksum = format(checksum_int, 'x').zfill(8)    #making sure it is 8 digits

    arguments_t0 = [file_id, fileName, checksum, new_file_path, _eos_destination, setup_label, monitor_fqc, jsn_file, run_number, lumiSection, streamName, fileSize, eventsNumber, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls, new_rundir_bad, esServerUrl, esIndexName, 5]
   #arguments_t0 = [file_id, fileName, checksum, new_file_path, _eos_destination, setup_label, monitor_fqc, jsn_file, run_number, lumiSection, streamName, fileSize, events_built, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls, new_rundir_bad, esServerUrl, esIndexName, 5]

    if dryrun:
        logger.info("Running in drymode, would have the file {0} to T0 with these arguments {1}".format(fileName,arguments_t0))
    else:
        apply_result = copyWorker.copyFile(file_id, fileName, checksum, new_file_path, _eos_destination, setup_label, monitor_fqc, jsn_file, run_number, lumiSection, streamName, fileSize, eventsNumber, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls, new_rundir_bad, esServerUrl, esIndexName, 5)
       #apply_result = copyWorker.copyFile(file_id, fileName, checksum, new_file_path, _eos_destination, setup_label, monitor_fqc, jsn_file, run_number, lumiSection, streamName, fileSize, events_built, eventsNumber, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls, new_rundir_bad, esServerUrl, esIndexName, 5)

def main():
    
    parser = argparse.ArgumentParser(description='Check the quality and status of a file or run')

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-f", "--filename" ,dest="filename"    , help="name of the file you want to check")
    group.add_argument("-r", "--runnumber",dest="runnumber"   , help="number of the run you want to check", type=int)

    parser.add_argument("-c", "--checkall"     ,dest="checkall"      , help="when set to True, all available infromation about the given run or file will be displayed",type=bool)

    parser.add_argument("-sm","--smstatus"     ,dest="smstatus"      , help="find the list of files with a given status and run, options are 'P5_INJECTED','TRANSFERRED','T0_CHECKED','T0_REPACKED'")
    parser.add_argument("-t0","--t0status"     ,dest="t0status"      , help="find the list of files with a given t0 status, options are 'CHECKED' and 'REPACKED'")
    parser.add_argument("-q", "--quality"      ,dest="file_quality"  , help="display the information about the size, built events, lost events for the specified file, options are True or False",type=bool)

    parser.add_argument("-x", "--xrdcp"        ,dest="xrdcp"         , help="copy a file to T0, options are True or False",type=bool)

    parser.add_argument("-us","--updatestatus" ,dest="updatestatus"  , help="updates to a given transfer status for the specified file, options are 'P5_INJECTED','TRANSFERRED','T0_CHECKED','T0_REPACKED'")
    parser.add_argument("-uq","--updatequality",dest="updatequality" , help="updates the file quality information for the specified file, options are True or False",type=bool)
    parser.add_argument("-ud","--updatedeleted",dest="updatedeleted" , help="updates the deleted flag status for the specified file, options are True or False",type=bool)  

    parser.add_argument("-d", "--dry-run"      , dest="dryrun"       , help="when set to True status updates will not be executed, the default value is False",type=bool)

    args = parser.parse_args()    

    if args.runnumber > 0 :

        n_injected    = checkRunStatus(1,args.runnumber)
        n_transferred = checkRunStatus(2,args.runnumber)
        n_checked     = checkRunStatus(3,args.runnumber)
        n_repacked    = checkRunStatus(4,args.runnumber)
        n_deleted     = checkDeletedStatus(args.runnumber)

        logger.info("Checking run {0} ...".format(args.runnumber))
        logger.info("      # of injected files    {0}".format(n_injected))
        logger.info("      # of transferred files {0}".format(n_transferred))
        logger.info("      # of checked files     {0}".format(n_checked))
        logger.info("      # of repacked files    {0}".format(n_repacked))
        logger.info("      # of deleted  files    {0}".format(n_deleted))

    if args.t0status is not None:
        findInconsistency(args.t0status)
        result_ids = findstatus_T0(args.t0status)
        logger.info("The list of files with status {0} in T0:".format(args.t0status))
        for counter in range(0,len(result_ids)):
            t0file =  copyWorker.getFileInfo(result_ids[counter][0],"",0)[1]
            logger.info("      {0}".format(t0file))

    if args.smstatus is not None and args.runnumber>0:
        result_ids = findstatus(args.smstatus,args.runnumber)
        logger.info("The list of files with status {0} in SM:".format(args.smstatus))
        for counter in range(0,len(result_ids)):            
            smfile =  copyWorker.getFileInfo(result_ids[counter][0],"",0)[1]
            logger.info("      {0}".format(smfile))

    if args.filename:
        logger.info("Checking file {0} ...".format(args.filename))

        fileid = copyWorker.getFileInfo(-1,args.filename,0)[0]
        logger.info("File id is {0} for file {1}".format(fileid,args.filename))
        
        if args.checkall:
            status = checkStatus(fileid)
            logger.info("The latest status for file {0} at SM is {1}".format(args.filename, status_flags[status]))

            status_t0_checked,status_t0_repacked = checkStatus_T0(fileid)
            if status_t0_checked is not False:
                logger.info("The latest status for file {0} at T0 is checked:{1}, repacked:{2}".format(args.filename, status_t0_checked,status_t0_repacked))            
            else:
                logger.info("The latest status for file {0} at T0 is not available ...".format(args.filename))            

            deletedstatus = checkDeletedStatus_perfile(fileid)
            logger.info("The latest status for file {0} for deletion is {1}".format(args.filename, deletedstatus))

        if args.updatestatus is not None:
            result_status  = updateStatus(args.updatestatus,fileid,args.dryrun)
            updated_status = checkStatus(fileid)
            logger.info("The new status for file {0} is {1}".format(args.filename, status_flags[updated_status]))

        if args.file_quality:
            if (checkFileQuality(args.filename)):
                filesize, eventsbuilt, eventsaccepted, eventslost, isgoodls =  checkFileQuality(args.filename)[0]
                logger.info("The file {0} has size of {1} MB, {2} built events, {3} accepted events, and {4} lost events. The lumi section is good: {5}".format(args.filename,filesize,eventsbuilt,eventsaccepted,eventslost,isgoodls))
            else:
                logger.info("File quality info is not filled for file {0}".format(args.filename))

        if args.updatequality:
            updateFileQuality(args.filename,args.dryrun)

        if args.xrdcp:
	    logger.info("Copying file {0} to T0".format(args.filename))
            copyFileToT0(args.filename,fileid,args.dryrun)

        if args.updatedeleted:
            result_deleted = updateDeleted(fileid,args.dryrun)
            logger.info("The deleted status for file {0} is updated".format(args.filename))

#______________________________________________________________________________                                                                                                                                                            
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
    #logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s in %(module)s: %(message)s')
    main()
