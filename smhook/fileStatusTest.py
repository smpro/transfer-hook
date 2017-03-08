#!/bin/env python

# Last modified by Dylan G. Hsu on 2015-05-29 :: dylan.hsu@cern.ch

import os,sys,socket,binascii
import shutil
import time,datetime
import cx_Oracle
import json
import logging
import random
import smhook.config
import smhook.databaseAgent as databaseAgent

# Hardcoded Config file to be used, is defined below:
# We read from production DB no matter what (in either case)
# but for testing, write to integration DB only
debug=False
cxn_timeout = 60*60 # Timeout for database connection in seconds
num_retries=5

logger = logging.getLogger(__name__)
# For debugging purposes, initialize the logger to stdout if running script as a standalone
if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)


def markAllRowsCompleted():
    query="UPDATE FILE_TRANSFER_STATUS SET STATUS_FLAG=4 WHERE STATUS_FLAG<>4"
    databaseAgent.runQuery('file_status',query,False)
    databaseAgent.cxn_db['file_status'].commit()

def testPolling(flag):
    n=1000
    distr_file = open("/nfshome0/dhsu/polling_times.dat", "w")
    for i in range(1,1000):
        markAllRowsCompleted()
        markRandomRows(n)
        query="SELECT FILENAME FROM FILE_TRANSFER_STATUS WHERE STATUS_FLAG={0}".format(flag)
        t1=int(round(time.time() * 1000000))/1000.
        result=databaseAgent.runQuery('file_status',query,True,custom_timeout=5)
        num_found=len( result )
        delta_t = int(round(time.time() * 1000000))/1000. - t1
        print "took {0} ms to poll and retrieve {1} filenames".format(delta_t, num_found)
        distr_file.write("{0} {1} {2}\n".format(i, num_found, delta_t))
    markAllRowsCompleted()
    distr_file.close()

def populateFtsTable(starting_runnumber=100001):
    stream="DGH"
    time0=int(round(time.time() * 1000))
    for runnumber in range(starting_runnumber,starting_runnumber+99999):
        print "inserting for runnumber {0}".format(runnumber)
        time_this_runnumber=int(round(time.time() * 1000))
        file_ids_string="file_ids: "
        for ls in range(0,999):
            connection=databaseAgent.useConnection('file_status')
            cursor=connection.cursor()
            cursor.callproc("dbms_output.enable", (None,))
            statusVar = cursor.var(cx_Oracle.NUMBER)
            lineVar   = cursor.var(cx_Oracle.STRING)
            filename="run{0}_ls{1}_stream{2}_dvmrg-c2f37-21-01.dat".format(runnumber,ls,stream)
            #query = "INSERT INTO CMS_STOMGR.FILE_TRANSFER_STATUS (FILE_ID, RUNNUMBER, LS, STREAM, FILENAME, CHECKSUM, STATUS_FLAG, DELETED_FLAG,INJECT_FLAG, BAD_CHECKSUM, P5_INJECTED_TIME) "+\
            #"VALUES (CMS_STOMGR.FILE_ID_SEQ.NEXTVAL, {0}, {1}, '{2}', '{3}', '{4}', {5}, {6}, {7}, {8}) ".format(
            #  runnumber,
            #  lumisection,
            #  stream,
            #  filename,
            #  binascii.b2a_hex(os.urandom(4)),
            #  4,
            #  1,
            #  1,
            #  0,
            #  "TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')",
            #)
            checksum = binascii.b2a_hex(os.urandom(4))
            query = "DECLARE ID_VAL NUMBER(27); "+\
            "BEGIN "+\
              "INSERT INTO CMS_STOMGR.FILE_TRANSFER_STATUS (FILE_ID, RUNNUMBER, LS,  STREAM, FILENAME, CHECKSUM, STATUS_FLAG, DELETED_FLAG, INJECT_FLAG, BAD_CHECKSUM, P5_INJECTED_TIME) VALUES "+\
              "(CMS_STOMGR.FILE_ID_SEQ.NEXTVAL,                      {0},       {1}, '{2}',  '{3}',    '{4}',    {5},         {6},          {7},         {8},          {9}             ) "+\
              "RETURNING FILE_ID INTO ID_VAL; "+\
              "COMMIT; "+\
              "DBMS_OUTPUT.PUT_LINE(ID_VAL); "+\
            "END;"
            query=query.format(runnumber, ls, stream, filename, checksum, 4,1,1,0, "TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')")
            result = databaseAgent.runQuery('file_status', query, False, custom_timeout=10)
            cursor.callproc("dbms_output.get_line", (lineVar, statusVar))
            got_file_id = statusVar.getvalue()==0
            file_id = int(lineVar.getvalue())
            file_ids_string = "{0} {1}, ".format(file_ids_string, file_id)
        print file_ids_string
        print ". . . took {0} ms".format( int(round(time.time() * 1000)) - time_this_runnumber)
    print "total time: {0} ms".format( int(round(time.time() * 1000)) - time0) 

def updateFtsTable(status_flag, runnumber_min, runnumber_max, machine_name):
    stream="DGH"
    time0=int(round(time.time() * 1000000))/1000.
    total_time=0
    distr_file = open("/nfshome0/dhsu/update_times_rn{0}-{1}_{2}.dat".format(runnumber_min, runnumber_max, machine_name), "w")
    list_of_ls=[]
    for runnumber in range(runnumber_min,runnumber_max):
        for lumisection in range(0,999):
            list_of_ls.append([runnumber,lumisection])
    random.shuffle(list_of_ls)
    for ls_pair in list_of_ls:
        runnumber=ls_pair[0]
        lumisection=ls_pair[1]
        filename="run{0}_ls{1}_stream{2}_dvmrg-c2f37-21-01.dat".format(runnumber,lumisection,stream)
        print "updating for runnumber {0}, ls {1}".format(runnumber, lumisection)
        query="UPDATE FILE_TRANSFER_STATUS SET STATUS_FLAG={0} WHERE FILENAME='{1}'".format(status_flag, filename)
        time_this_lumisection=int(round(time.time() * 1000000))/1000.
        databaseAgent.runQuery('file_status',query,False)
        databaseAgent.cxn_db['file_status'].commit()
        delta_t = int(round(time.time() * 1000000))/1000. - time_this_lumisection
        total_time+=delta_t
        query="SELECT 1 FROM DUAL"
        time_control_query=int(round(time.time() * 1000000))/1000.
        databaseAgent.runQuery('file_status',query,False)
        delta_t_control=int(round(time.time() * 1000000))/1000. - time_control_query
        print ". . . took {0} ms (dummy query took {1} ms)".format(delta_t, delta_t_control)
        distr_file.write("{0} {1} {2} {3}\n".format(runnumber, lumisection, delta_t, delta_t_control))
    print "sum of all update query times: {0} ms".format(total_time)
    distr_file.close()

# This method is used for testing only and should not be run in the production environment, ever!
# Also serves as internal documentation of the necessary tables because this stuff is not written down anywhere...
def makeTestTables():
#      STATUS_FLAG ENUM('P5_OPENED', 'P5_CLOSED', 'T0_INJECTED', 'TRANSFERRED', 'T0_CHECKED', 'T0_REPACKED', 'P5_DELETED'),
    q_sequence1="BEGIN "+\
      "EXECUTE IMMEDIATE 'CREATE SEQUENCE FILE_ID_SEQ "+\
      "MINVALUE 1 "+\
      "MAXVALUE 999999999999999999999999999 "+\
      "START WITH 1 "+\
      "INCREMENT BY 1 "+\
      "CACHE 20'; "+\
      "EXECUTE IMMEDIATE 'GRANT SELECT ON FILE_ID_SEQ to CMS_STOMGR_W'; "+\
      "END;"

    q_table1="BEGIN "+\
      "EXECUTE IMMEDIATE 'CREATE TABLE FILE_TRANSFER_STATUS ( "+\
        "FILE_ID               NUMBER(27)     NOT NULL, "+\
        "RUNNUMBER             NUMBER(22)     NOT NULL, "+\
        "LS                    NUMBER(22)     NOT NULL, "+\
        "STREAM                VARCHAR2(256)  NOT NULL, "+\
        "FILENAME              VARCHAR2(1000) NOT NULL, "+\
        "CHECKSUM              VARCHAR2(50), "+\
        "P5_INJECTED_TIME      TIMESTAMP(6), "+\
        "TRANSFER_START_TIME   TIMESTAMP(6), "+\
        "TRANSFER_END_TIME     TIMESTAMP(6), "+\
        "T0_CHECKED_TIME       TIMESTAMP(6), "+\
        "T0_REPACKED_TIME      TIMESTAMP(6), "+\
        "P5_DELETED_TIME       TIMESTAMP(6), "+\
        "STATUS_FLAG           NUMBER(1) NOT NULL, "+\
        "DELETED_FLAG          NUMBER(1) NOT NULL, "+\
        "INJECT_FLAG           NUMBER(1) NOT NULL, "+\
        "BAD_CHECKSUM          NUMBER(1) NOT NULL, "+\
        "PRIMARY KEY (FILE_ID), "+\
        "CONSTRAINT CONS_FTS_FILENAME UNIQUE(FILENAME), "+\
        "CONSTRAINT CONS_FTS_LUMIS UNIQUE(RUNNUMBER, LS, STREAM) "+\
      ")'; "+\
      "EXECUTE IMMEDIATE 'GRANT ALL PRIVILEGES ON FILE_TRANSFER_STATUS TO CMS_STOMGR_W'; "+\
      "EXECUTE IMMEDIATE 'GRANT SELECT ON FILE_TRANSFER_STATUS TO PUBLIC'; "+\
      "EXECUTE IMMEDIATE 'CREATE INDEX IDX_FTS_FLAGS "+\
        "ON FILE_TRANSFER_STATUS ( "+\
          "STATUS_FLAG "+\
        ")'; "+\
    "END;"
    q_table2="CREATE FUNCTION T0_NEEDS_TO_INJECT (STATUS_FLAG IN NUMBER, INJECT_FLAG IN NUMBER, BAD_CHECKSUM IN NUMBER) "+\
      "RETURN INT DETERMINISTIC IS "+\
      "BEGIN "+\
        "IF ( STATUS_FLAG=2 AND INJECT_FLAG=1 AND BAD_CHECKSUM=0 ) THEN RETURN 0; "+\
        "ELSE RETURN NULL; "+\
        "END IF; "+\
      "END T0_NEEDS_TO_INJECT;"
    q_table3="CREATE INDEX IDX_NEW_FILES_FOR_T0 ON FILE_TRANSFER_STATUS (T0_NEEDS_TO_INJECT(STATUS_FLAG, INJECT_FLAG, BAD_CHECKSUM))"
    q_table4="BEGIN "+\
      "EXECUTE IMMEDIATE 'CREATE TABLE FILE_QUALITY_CONTROL ( "+\
      "RUNNUMBER                  NUMBER(22)        NOT NULL, "+\
      "LS                         NUMBER(22)        NOT NULL, "+\
      "STREAM                     VARCHAR2(256)      NOT NULL, "+\
      "FILENAME                   VARCHAR2(1000)    NOT NULL, "+\
      "LAST_UPDATE_TIME           TIMESTAMP(6), "+\
      "FILE_SIZE                  NUMBER(22), "+\
      "EVENTS_BUILT               NUMBER(22), "+\
      "EVENTS_LOST                NUMBER(22), "+\
      "EVENTS_LOST_CHECKSUM       NUMBER(22),        "+\
      "EVENTS_LOST_CMSSW          NUMBER(22),        "+\
      "EVENTS_LOST_CRASH          NUMBER(22),        "+\
      "EVENTS_LOST_OVERSIZED      NUMBER(22),        "+\
      "IS_GOOD_LS                 NUMBER(1)         NOT NULL, "+\
      "PRIMARY KEY (FILENAME) "+\
    ")'; "+\
    "EXECUTE IMMEDIATE 'GRANT ALL PRIVILEGES ON FILE_QUALITY_CONTROL TO CMS_STOMGR_W'; "+\
    "EXECUTE IMMEDIATE 'GRANT SELECT ON FILE_QUALITY_CONTROL TO PUBLIC'; "+\
    "END;"

    try:
        databaseAgent.runQuery('CMS_STOMGR', q_sequence1, False)
        databaseAgent.runQuery('CMS_STOMGR', q_table1, False)
        databaseAgent.runQuery('CMS_STOMGR', q_table2, False)
        databaseAgent.cxn_db['CMS_STOMGR'].commit()
        databaseAgent.runQuery('CMS_STOMGR', q_table3, False)
        databaseAgent.cxn_db['CMS_STOMGR'].commit()
        databaseAgent.runQuery('CMS_STOMGR', q_table4, False)
        databaseAgent.cxn_db['CMS_STOMGR'].commit()
        #databaseAgent.runQuery('CMS_STOMGR', q_view1, False)
        #databaseAgent.runQuery('CMS_STOMGR', q_view2, False)
        #databaseAgent.cxn_db['CMS_STOMGR'].commit()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print error.code
        print error.message
        print error.context
        print error.offset
        print query[max(0,error.offset-20):min(len(query)-1,error.offset+20)]

def makeViews():
  q_mview_status_streams="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW LOG ON CMS_STOMGR.FILE_TRANSFER_STATUS
    WITH PRIMARY KEY, ROWID  (RUNNUMBER, STREAM, STATUS_FLAG, INJECT_FLAG, BAD_CHECKSUM, DELETED_FLAG)
    INCLUDING NEW VALUES';
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW STATUS_STREAMS
      CACHE
      BUILD IMMEDIATE
      REFRESH FAST ON COMMIT
    AS SELECT 
      RUNNUMBER, STREAM,
      SUM(IS_INJECTED)    AS FILES_INJECTED,
      SUM(IS_TRANSFERRED) AS FILES_TRANSFERRED,
      SUM(IS_CHECKED)     AS FILES_CHECKED,
      SUM(IS_REPACKED)    AS FILES_REPACKED,
      SUM(IS_DELETED)     AS FILES_DELETED,
      SUM(IS_CORRUPTED)   AS FILES_CORRUPTED
    FROM (
      SELECT
        RUNNUMBER,
        STREAM,
        DECODE(STATUS_FLAG,1,1,0) AS IS_INJECTED,
        DECODE(STATUS_FLAG,2,1,0) AS IS_TRANSFERRED,
        DECODE(STATUS_FLAG,3,1,0) AS IS_CHECKED,
        DECODE(STATUS_FLAG,4,1,0) AS IS_REPACKED,
        DELETED_FLAG AS IS_DELETED,
        BAD_CHECKSUM AS IS_CORRUPTED
        FROM FILE_TRANSFER_STATUS WHERE INJECT_FLAG=1 AND RUNNUMBER<=999999999
    )
    GROUP BY RUNNUMBER, STREAM
    ORDER BY RUNNUMBER DESC, STREAM ASC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_STREAMS TO PUBLIC';
    END;
  """
  q_view_status_runs="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE VIEW STATUS_RUNS AS
    SELECT
      RUNNUMBER,
      SUM(FILES_INJECTED)    AS FILES_INJECTED_RUN,
      SUM(FILES_TRANSFERRED) AS FILES_TRANSFERRED_RUN,
      SUM(FILES_CHECKED)     AS FILES_CHECKED_RUN,
      SUM(FILES_REPACKED)    AS FILES_REPACKED_RUN,
      SUM(FILES_DELETED)     AS FILES_DELETED_RUN,
      SUM(FILES_CORRUPTED)   AS FILES_CORRUPTED_RUN
    FROM CMS_STOMGR.STATUS_STREAMS
    GROUP BY RUNNUMBER
    ORDER BY RUNNUMBER DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_RUNS TO PUBLIC';
    END;
  """
  q_mview_run_file_quality="""
    BEGIN 
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW LOG ON CMS_STOMGR.FILE_QUALITY_CONTROL
    WITH PRIMARY KEY, ROWID  (RUNNUMBER, EVENTS_BUILT, EVENTS_LOST, EVENTS_LOST_CHECKSUM, EVENTS_LOST_CMSSW, EVENTS_LOST_CRASH, EVENTS_LOST_OVERSIZED, FILE_SIZE)
    INCLUDING NEW VALUES';
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW RUN_FILE_QUALITY
      CACHE
      BUILD IMMEDIATE
      REFRESH FAST ON COMMIT
    AS SELECT
    RUNNUMBER, 
      SUM(NVL(EVENTS_BUILT,0))          AS TOTAL_EVENTS_BUILT         , 
      SUM(NVL(EVENTS_LOST,0))           AS TOTAL_EVENTS_LOST          , 
      SUM(NVL(EVENTS_LOST_CHECKSUM,0))  AS TOTAL_EVENTS_LOST_CHECKSUM , 
      SUM(NVL(EVENTS_LOST_CMSSW,0))     AS TOTAL_EVENTS_LOST_CMSSW    , 
      SUM(NVL(EVENTS_LOST_CRASH,0))     AS TOTAL_EVENTS_LOST_CRASH    , 
      SUM(NVL(EVENTS_LOST_OVERSIZED,0)) AS TOTAL_EVENTS_LOST_OVERSIZED, 
      SUM(NVL(FILE_SIZE,0))             AS TOTAL_SIZE 
    FROM CMS_STOMGR.FILE_QUALITY_CONTROL WHERE RUNNUMBER<999999999 AND EVENTS_BUILT>=0 
    GROUP BY RUNNUMBER
    ORDER BY RUNNUMBER DESC'; 
    EXECUTE IMMEDIATE 'GRANT SELECT ON RUN_FILE_QUALITY TO PUBLIC'; 
    END;
  """
  databaseAgent.runQuery('CMS_STOMGR', q_mview_status_streams);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()
  databaseAgent.runQuery('CMS_STOMGR', q_view_status_runs);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()
  databaseAgent.runQuery('CMS_STOMGR', q_mview_run_file_quality);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()

# Don't run this in the production environment, ever.
def dropViews():
    databaseAgent.runQuery('CMS_STOMGR', "DROP VIEW STATUS_STREAMS", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP VIEW STATUS_RUNS", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP VIEW RUN_FILE_QUALITY", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP MATERIALIZED VIEW STATUS_STREAMS", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP MATERIALIZED VIEW RUN_FILE_QUALITY", False)
    databaseAgent.cxn_db['CMS_STOMGR'].commit()
# Don't run this in the production environment, ever.
def dropTestTables():
    databaseAgent.runQuery('CMS_STOMGR', "DROP SEQUENCE FILE_ID_SEQ", False)
    databaseAgent.runQuery('CMS_STOMGR', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'FILE_TRANSFER_STATUS'; if existing_tables > 0 then execute immediate 'drop table FILE_TRANSFER_STATUS'; end if; end;", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP FUNCTION T0_NEEDS_TO_INJECT")
    databaseAgent.cxn_db['CMS_STOMGR'].commit()
