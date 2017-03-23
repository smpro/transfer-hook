#!/bin/env python

# Last modified by Dylan G. Hsu on 2016-03-07 :: dylan.hsu@cern.ch

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json
import logging

import smhook.config
import smhook.databaseAgent as databaseAgent

# Hardcoded settings
debug=False

logger = logging.getLogger(__name__)
# For debugging purposes, initialize the logger to stdout if running script as a standalone
if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

#############################
# fileQualityControl        #
#############################
# This function takes a full path to a json file, the filename of a data file, and several numeric arguments then inserts or updates the relevant information in the database
# The number of events built is greater than or equal to number of events lost.

def fileQualityControl(data_file, run_number, ls, stream, file_size, events_built, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls):
    events_built = max(events_built, 0) # Protect against files with -1 events built and lost
    events_lost = max(min(events_built, events_lost_checksum + events_lost_cmssw + events_lost_crash + events_lost_oversized),0)
    
    query="SELECT FILENAME FROM CMS_STOMGR.FILE_QUALITY_CONTROL WHERE FILENAME='"+data_file+"'"
    # See if there is an existing row
    result = databaseAgent.runQuery('file_status', query, True)
    #if len(result)==0:
    #    return False
    if(is_good_ls):
        is_good_ls=1
    else:
        is_good_ls=0
    if len(result)==0:
        # No existing row. we must now try to insert:
        query="""
         BEGIN 
            INSERT INTO CMS_STOMGR.FILE_QUALITY_CONTROL (
                RUNNUMBER,
                LS,
                STREAM,
                FILENAME,
                LAST_UPDATE_TIME,
                FILE_SIZE,
                EVENTS_BUILT,
                EVENTS_LOST,
                EVENTS_LOST_CHECKSUM,
                EVENTS_LOST_CMSSW,
                EVENTS_LOST_CRASH,
                EVENTS_LOST_OVERSIZED,
                IS_GOOD_LS
            ) VALUES (
                {1}, {2}, '{3}', '{4}', {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}
            );
            COMMIT;
         END;
        """
        query=query.format(
            "CMS_STOMGR.FILE_QUALITY_CONTROL",
            run_number,
            ls,
            stream,
            data_file,
            "TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", #UTC timestamp -> oracle
            file_size,
            events_built,
            events_lost,
            events_lost_checksum,
            events_lost_cmssw,
            events_lost_crash,
            events_lost_oversized,
            is_good_ls
        )
    else:
        # Update the existing row
        query="""
         BEGIN
            UPDATE CMS_STOMGR.FILE_QUALITY_CONTROL SET
                RUNNUMBER              = {1},
                LS                     = {2},
                STREAM                 = '{3}',
                LAST_UPDATE_TIME       = {5},
                FILE_SIZE              = {6},
                EVENTS_BUILT           = {7},
                EVENTS_LOST            = {8},
                EVENTS_LOST_CHECKSUM   = {9},
                EVENTS_LOST_CMSSW      = {10},
                EVENTS_LOST_CRASH      = {11},
                EVENTS_LOST_OVERSIZED  = {12},
                IS_GOOD_LS             = {13}
            WHERE FILENAME='{4}';
            COMMIT;
         END;
        """
        query=query.format(
            "CMS_STOMGR.FILE_QUALITY_CONTROL",
            run_number,
            ls,
            stream,
            data_file,
            "TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", #UTC timestamp -> oracle
            file_size,
            events_built,
            events_lost,
            events_lost_checksum,
            events_lost_cmssw,
            events_lost_crash,
            events_lost_oversized,
            is_good_ls
        )
    
    result=databaseAgent.runQuery('file_status', query, False)
    if result is False:
        return False
    return True
