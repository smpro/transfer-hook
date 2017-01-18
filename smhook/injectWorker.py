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
    cursor.callproc("dbms_output.enable", (None,))
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
def recordTransferStart(file_id):
    query="UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET TRANSFER_START_TIME={0} "+\
      "WHERE FILE_ID={1}"
    query=query.format("TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", file_id)
    result = databaseAgent.runQuery('file_status', query, fetch_output=False)
    return result
#______________________________________________________________________________
def recordTransferComplete(file_id):
    query="UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET TRANSFER_END_TIME = {0}, "+\
      "STATUS_FLAG = {1}, "+\
      "BAD_CHECKSUM = 0 "+\
      "WHERE FILE_ID={2}"
    query=query.format("TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", status_flags['TRANSFERRED'], file_id)
    result = databaseAgent.runQuery('file_status', query, fetch_output=False)
    return result
#______________________________________________________________________________
def recordCorruptedTransfer(file_id):
    query="UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET TRANSFER_END_TIME = {0}, "+\
      "STATUS_FLAG = {1}, "+\
      "BAD_CHECKSUM = 1 "+\
      "WHERE FILE_ID={2}"
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

