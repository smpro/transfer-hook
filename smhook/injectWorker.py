#!/bin/env python

# Last modified by Dylan G. Hsu on 2015-05-29 :: dylan.hsu@cern.ch

import os,sys,socket
import shutil
import time,datetime
import json
import logging
import signal
#import multiprocessing

import smhook.databaseAgent as databaseAgent

import cx_Oracle

# Hardcoded Config file to be used, is defined below:
# We read from production DB no matter what (in either case)
# but for testing, write to integration DB only
debug=True

logger = logging.getLogger(__name__)

status_flags = {
  'P5_INJECTED': 1,
  'TRANSFERRED': 2,
  'T0_CHECKED' : 3,
  'T0_REPACKED': 4
}

# For debugging purposes, initialize the logger to stdout if running script as a standalone
if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

def insertFile(filename, runnumber, ls, stream, checksum, inject_into_T0=True):
    # Inserts a new file into the system
    # Need to account for cases where file is already in the system!
    
    connection=databaseAgent.useConnection('file_status')
    cursor=connection.cursor()

    # Our connections are opened in mode DBMS_OUTPUT.ENABLE so we can insert a file and get the FILE_ID in one query 
    # Looking it up afterwards would be too expensive
    # This is now taken care of when databaseAgent creates the connections

    statusVar = cursor.var(cx_Oracle.NUMBER)
    lineVar   = cursor.var(cx_Oracle.STRING)

    if inject_into_T0:
      inject_flag=1
    else:
      inject_flag=0

    query = "DECLARE ID_VAL NUMBER(27); "+\
    "BEGIN "+\
      "INSERT INTO CMS_STOMGR.FILE_TRANSFER_STATUS (FILE_ID, RUNNUMBER, LS,  STREAM, FILENAME, CHECKSUM, STATUS_FLAG, DELETED_FLAG, INJECT_FLAG, BAD_CHECKSUM, P5_INJECTED_TIME) VALUES "+\
      "(CMS_STOMGR.FILE_ID_SEQ.NEXTVAL,                      {0},       {1}, '{2}',  '{3}',    '{4}',    {5},         {6},          {7},         {8},          {9}             ) "+\
      "RETURNING FILE_ID INTO ID_VAL; "+\
      "COMMIT; "+\
      "DBMS_OUTPUT.PUT_LINE(ID_VAL); "+\
    "END;"
    query=query.format(runnumber, ls, stream, filename, checksum, 1, 0, inject_flag, 0, "TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')")
    #print query
    result = databaseAgent.runQuery('file_status', query, False)
    cursor.callproc("dbms_output.get_line", (lineVar, statusVar))
    got_file_id = statusVar.getvalue()==0
    #print "statusVar={0}".format(statusVar.getvalue())
    #print "lineVar={0}".format(lineVar.getvalue())
    if got_file_id is False or result is False:
      return False
    else:
      file_id = int(lineVar.getvalue())
      return file_id
#______________________________________________________________________________
def recordTransferStart(file_id,lfn):
    query="BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET TRANSFER_START_TIME={0}, "+\
      "PATH={1}"+\
      "WHERE FILE_ID={2}; COMMIT; END;"
    query=query.format("TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", "'"+lfn+"'", file_id)
    result = databaseAgent.runQuery('file_status', query, fetch_output=False)
    return result
#______________________________________________________________________________
def recordTransferComplete(file_id):
    query="BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET TRANSFER_END_TIME = {0}, "+\
      "STATUS_FLAG = {1}, "+\
      "BAD_CHECKSUM = 0 "+\
      "WHERE FILE_ID={2}; COMMIT; END;"
    query=query.format("TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", status_flags['TRANSFERRED'], file_id)
    result = databaseAgent.runQuery('file_status', query, fetch_output=False)
    return result
#______________________________________________________________________________
def recordFileDeleted(file_id):
    query="BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET DELETED_FLAG = {0} "+\
      "WHERE FILE_ID={1}; COMMIT; END;"
    query=query.format(1, file_id)
    result = databaseAgent.runQuery('file_status', query, fetch_output=False)
    return result
#______________________________________________________________________________
def recordCorruptedTransfer(file_id):
    query="BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET TRANSFER_END_TIME = {0}, "+\
      "STATUS_FLAG = {1}, "+\
      "BAD_CHECKSUM = 1 "+\
      "WHERE FILE_ID={2}; COMMIT; END;"
    query=query.format("TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", status_flags['TRANSFERRED'], file_id)
    result = databaseAgent.runQuery('file_status', query, fetch_output=False)
    return result
#______________________________________________________________________________
def __pollT0ForFiles(search_flag, unmarked_flag, stream_blacklist=[], runnumber_blacklist=[]):
    # Find rows in the T0 table with the search flag marked and the unmarked flag not marked
    query = "SELECT FILENAME FROM CMS_STOMGR.FILE_TRANSFER_STATUS WHERE"+\
      "BITAND(STATUS_FLAG,{0})={0} AND BITAND(STATUS_FLAG,{1})=0".format(search_flag,unmarked_flag)
    if len(stream_blacklist) > 0:
        query += " AND STREAM NOT IN ('" + "', '".join(stream_blacklist) + "')"
    if len(runnumber_blacklist) > 0:
        query += " AND RUNNUMBER NOT IN (" + ", ".join(runnumber_blacklist) + ")"
    result=databaseAgent.runQuery('file_status_T0', query, true)
    return result
#______________________________________________________________________________
def findT0Files(status,changeStatus=False):

    result_statusUpdate = False
    
    if status is "checked":
        flag    = "CHECKED_RETRIEVE"
        flag_SM = "T0_CHECKED"
    elif status is "repacked":
        flag    = "REPACKED_RETRIEVE"
        flag_SM = "T0_REPACKED"
    else:
        logger.warning("Status to find files in the T0 tables is not set, exiting...")
        return False

    # Find rows in the T0 table with the search flag marked and the unmarked flag not marked
    #query = "SELECT P5_FILEID FROM CMS_T0DATASVC_REPLAY2.FILE_TRANSFER_STATUS_OFFLINE WHERE "+flag+"=1"
    query = "SELECT P5_FILEID FROM CMS_T0DATASVC_PROD.FILE_TRANSFER_STATUS_OFFLINE WHERE "+flag+"=1"
    result_fileId=databaseAgent.runQuery('file_status_T0', query, fetch_output=True)

    logger.info("Found {0} files in T0 with status {1}".format(len(result_fileId),status))

    if changeStatus and len(result_fileId)>0:
        for l in range(0,len(result_fileId)):
            query_statusUpdate_SM = "BEGIN UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS " +\
                                    "SET STATUS_FLAG={0} " +\
                                    "WHERE FILE_ID={1}; COMMIT; END;"
            query_statusUpdate_SM = query_statusUpdate_SM.format(status_flags[flag_SM], result_fileId[l][0])
            result_statusUpdate_SM= databaseAgent.runQuery('file_status', query_statusUpdate_SM, fetch_output=False)

            #query_statusUpdate = "BEGIN UPDATE CMS_T0DATASVC_REPLAY2.FILE_TRANSFER_STATUS_OFFLINE "+\
            query_statusUpdate = "BEGIN UPDATE CMS_T0DATASVC_PROD.FILE_TRANSFER_STATUS_OFFLINE "+\
                                 "SET "+flag+"={0} "+\
                                 "WHERE P5_FILEID={1}; COMMIT; END;"
            #not sure how to insert null
            query_statusUpdate =query_statusUpdate.format('NULL',result_fileId[l][0])
            result_statusUpdate=databaseAgent.runQuery('file_status_T0', query_statusUpdate, fetch_output=False)

    logger.info("Updated {0} files in T0 with status {1}".format(len(result_fileId),status))

    #return result_fileId, result_statusUpdate, result_statusUpdate_SM

