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

# Hardcoded Config file to be used, is defined below:
# We read from production DB no matter what (in either case)
# but for testing, write to integration DB only
debug=True

logger = logging.getLogger(__name__)
# For debugging purposes, initialize the logger to stdout if running script as a standalone
if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

def insertFile(filename, runnumber, stream, ls):
    # Inserts a new file with status flag "open" into the system
    query = "INSERT INTO CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "(RUNNUMBER, LS, STREAM, FILENAME, LAST_UPDATE_TIME, STATUS_FLAG) "+\
      "VALUES "+\
      "({0}, {1], '{2}', '{3}', {4}, {5})".format(
        runnumber,
        lumisection,
        stream,
        filename,
        "TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')",
        "(SELECT STATUS_FLAG FROM FILE_STATUS_FLAGS WHERE MEANING='P5_OPENED')"
      )
    databaseAgent.runQuery('file_status', query, false)
    return true
def closedFile(filename):
    # Mark as closed a file that already exists in 'open' state in the database
    return __updateFile(filename, 'P5_OPENED', 'P5_CLOSED')
def copiedFile(filename):
    # Mark a file as transferred after successfully copying it
    return __updateFile(filename, 'T0_INJECTED', 'TRANSFERRED')
def deletedFile(filename, repacked=True)
    # Mark a file as deleted after it has been repacked or checked (depending on the deletion policy)
    if repacked:
        old_flag='P5_REPACKED'
    else:
        old_flag='P5_CHECKED'
    return __updateFile(filename, old_flag, 'P5_DELETED')
def getInjectedFiles(stream_blacklist=[], runnumber_blacklist=[]):
    result=__pollT0ForFiles('T0_INJECTED', stream_blacklist, runnumber_blacklist)
    return result
def getCheckedFiles(stream_blacklist=[], runnumber_blacklist=[]):
    result=__pollT0ForFiles('T0_CHECKED', stream_blacklist, runnumber_blacklist)
    return result
def getRepackedFiles(stream_blacklist=[], runnumber_blacklist=[]):
    result=__pollT0ForFiles('T0_REPACKED', stream_blacklist, runnumber_blacklist)
    return result
def __updateFile(filename, old_flag, new_flag):
    # Update a file that is already in the database
    query = ("SELECT COUNT(*) FROM CMS_STOMGR.FILE_TRANSFER_STATUS WHERE "+\
      "STATUS_FLAG={0) AND "+\
      "FILENAME='{1}'").format(
        "(SELECT STATUS_FLAG FROM FILE_STATUS_FLAGS WHERE MEANING='{0}')".format(old_flag),
        filename
      )
    result=databaseAgent.runQuery('file_status', query, true)
    if result[0][0] == 0:
        return false
    query = ("UPDATE CMS_STOMGR.FILE_TRANSFER_STATUS "+\
      "SET STATUS_FLAG={0} WHERE "+\
      "STATUS_FLAG={1} AND "+\
      "FILENAME='{2}'").format(
        "(SELECT STATUS_FLAG FROM FILE_STATUS_FLAGS WHERE MEANING='{0}')".format(new_flag),
        "(SELECT STATUS_FLAG FROM FILE_STATUS_FLAGS WHERE MEANING='{0}')".format(old_flag),
        filename
      )
    databaseAgent.runQuery('file_status', query, false)
    return true
def __pollT0ForFiles(flag, stream_blacklist=[], runnumber_blacklist=[], operator="="):
    query = "SELECT FILENAME FROM CMS_STOMGR.FILE_TRANSFER_STATUS WHERE"+\
      "STATUS_FLAG {0} (SELECT STATUS_FLAG FROM FILE_STATUS_FLAGS WHERE MEANING='{1}')".format(operator, flag)
    if len(stream_blacklist) > 0:
        query += " AND STREAM NOT IN ('" + "', '".join(stream_blacklist) + "')"
    if len(runnumber_blacklist) > 0:
        query += " AND RUNNUMBER NOT IN (" + ", ".join(runnumber_blacklist) + ")"
    result=database.Agent.runQuery('file_status_T0', query, true)
    return result

