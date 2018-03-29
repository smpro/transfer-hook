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

    q_table1="""BEGIN 
      EXECUTE IMMEDIATE 'CREATE TABLE FILE_TRANSFER_STATUS ( 
        PATH                  VARCHAR2(1000) NOT NULL,
        FILE_ID               NUMBER(27)     NOT NULL, 
        RUNNUMBER             NUMBER(22)     NOT NULL, 
        LS                    NUMBER(22)     NOT NULL, 
        STREAM                VARCHAR2(256)  NOT NULL, 
        FILENAME              VARCHAR2(1000) NOT NULL, 
        CHECKSUM              VARCHAR2(50), 
        P5_INJECTED_TIME      TIMESTAMP(6), 
        TRANSFER_START_TIME   TIMESTAMP(6), 
        TRANSFER_END_TIME     TIMESTAMP(6), 
        T0_CHECKED_TIME       TIMESTAMP(6), 
        T0_REPACKED_TIME      TIMESTAMP(6), 
        P5_DELETED_TIME       TIMESTAMP(6), 
        STATUS_FLAG           NUMBER(1) NOT NULL, 
        DELETED_FLAG          NUMBER(1) NOT NULL, 
        INJECT_FLAG           NUMBER(1) NOT NULL, 
        BAD_CHECKSUM          NUMBER(1) NOT NULL, 
        PRIMARY KEY (FILE_ID), 
        CONSTRAINT CONS_FTS_FILENAME UNIQUE(FILENAME), 
        CONSTRAINT CONS_FTS_LUMIS UNIQUE(RUNNUMBER, LS, STREAM) 
      )'; 
      EXECUTE IMMEDIATE 'GRANT ALL PRIVILEGES ON FILE_TRANSFER_STATUS TO CMS_STOMGR_W'; 
      EXECUTE IMMEDIATE 'GRANT SELECT ON FILE_TRANSFER_STATUS TO PUBLIC'; 
      EXECUTE IMMEDIATE 'CREATE INDEX IDX_FTS_FLAGS 
        ON FILE_TRANSFER_STATUS ( 
          STATUS_FLAG 
        )'; 
    END;"""
    q_table2="CREATE FUNCTION T0_NEEDS_TO_INJECT (STATUS_FLAG IN NUMBER, INJECT_FLAG IN NUMBER, BAD_CHECKSUM IN NUMBER) "+\
      "RETURN INT DETERMINISTIC IS "+\
      "BEGIN "+\
        "IF ( STATUS_FLAG=2 AND INJECT_FLAG=1 AND BAD_CHECKSUM=0 ) THEN RETURN 0; "+\
        "ELSE RETURN NULL; "+\
        "END IF; "+\
      "END T0_NEEDS_TO_INJECT;"+\
      "EXECUTE IMMEDIATE 'GRANT EXECUTE ON T0_NEEDS_TO_INJECT TO CMS_STOMGR_TIER0_R'; "
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
      "EVENTS_ACCEPTED            NUMBER(22), "+\
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

def makeDummyViews():
  # views that work in the integration environment
  # OUT OF DATE
  q_mview_status_streams="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW LOG ON CMS_STOMGR.FILE_TRANSFER_STATUS
    WITH PRIMARY KEY, ROWID  (RUNNUMBER, STREAM, STATUS_FLAG, INJECT_FLAG, BAD_CHECKSUM, DELETED_FLAG, P5_INJECTED_TIME)
    INCLUDING NEW VALUES';
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW MVIEW_STATUS_STREAMS
      CACHE
      BUILD IMMEDIATE
      REFRESH FAST ON COMMIT
    AS SELECT 
      RUNNUMBER, STREAM,
      SUM(IS_INJECTED)      AS FILES_INJECTED,
      SUM(IS_TRANSFERRED)   AS FILES_TRANSFERRED,
      SUM(IS_CHECKED)       AS FILES_CHECKED,
      SUM(IS_REPACKED)      AS FILES_REPACKED,
      SUM(IS_DELETED)       AS FILES_DELETED,
      SUM(IS_CORRUPTED)     AS FILES_CORRUPTED,
      MAX(P5_INJECTED_TIME) AS LAST_INJECTION_TIME,
      COUNT(IS_INJECTED),      
      COUNT(IS_TRANSFERRED),
      COUNT(IS_CHECKED),  
      COUNT(IS_REPACKED),      
      COUNT(IS_DELETED),       
      COUNT(IS_CORRUPTED),
      COUNT(*)
    FROM (
      SELECT
        RUNNUMBER,
        STREAM,
        P5_INJECTED_TIME,
        DECODE(STATUS_FLAG,1,1,0) AS IS_INJECTED,
        DECODE(STATUS_FLAG,2,1,0) AS IS_TRANSFERRED,
        DECODE(STATUS_FLAG,3,1,0) AS IS_CHECKED,
        DECODE(STATUS_FLAG,4,1,0) AS IS_REPACKED,
        DELETED_FLAG AS IS_DELETED,
        BAD_CHECKSUM AS IS_CORRUPTED
        FROM FILE_TRANSFER_STATUS WHERE INJECT_FLAG=1
    )
    GROUP BY RUNNUMBER, STREAM
    ORDER BY LAST_INJECTION_TIME DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON MVIEW_STATUS_STREAMS TO PUBLIC';
    END;
  """
  q_view_status_streams="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE VIEW STATUS_STREAMS AS
    SELECT
      STREAMS.RUNNUMBER AS RUNNUMBER,
      STREAMS.STREAM AS STREAM,
      STREAMS.FILES_INJECTED AS FILES_INJECTED,
      STREAMS.FILES_TRANSFERRED AS FILES_TRANSFERRED,
      STREAMS.FILES_CHECKED AS FILES_CHECKED,
      STREAMS.FILES_REPACKED AS FILES_REPACKED,
      STREAMS.FILES_INJECTED + STREAMS.FILES_TRANSFERRED + STREAMS.FILES_CHECKED + STREAMS.FILES_REPACKED AS FILES_TOTAL,
      STREAMS.FILES_DELETED AS FILES_DELETED,
      STREAMS.FILES_CORRUPTED AS FILES_CORRUPTED,
      STREAMS.LAST_INJECTION_TIME AS LAST_INJECTION_TIME,
      SFQ.TOTAL_SIZE AS TOTAL_SIZE,
      SFQ.TOTAL_EVENTS_BUILT AS TOTAL_EVENTS_BUILT
    FROM
    (
      SELECT
        CMS_STOMGR.MVIEW_STATUS_STREAMS.RUNNUMBER              AS RUNNUMBER,
        CMS_STOMGR.MVIEW_STATUS_STREAMS.STREAM                 AS STREAM,
        SUM(CMS_STOMGR.MVIEW_STATUS_STREAMS.FILES_INJECTED)    AS FILES_INJECTED,
        SUM(CMS_STOMGR.MVIEW_STATUS_STREAMS.FILES_TRANSFERRED) AS FILES_TRANSFERRED,
        SUM(CMS_STOMGR.MVIEW_STATUS_STREAMS.FILES_CHECKED)     AS FILES_CHECKED,
        SUM(CMS_STOMGR.MVIEW_STATUS_STREAMS.FILES_REPACKED)    AS FILES_REPACKED,
        SUM(CMS_STOMGR.MVIEW_STATUS_STREAMS.FILES_DELETED)     AS FILES_DELETED,
        SUM(CMS_STOMGR.MVIEW_STATUS_STREAMS.FILES_CORRUPTED)   AS FILES_CORRUPTED,
        MAX(LAST_INJECTION_TIME)                         AS LAST_INJECTION_TIME
      FROM CMS_STOMGR.MVIEW_STATUS_STREAMS
      GROUP BY RUNNUMBER, STREAM
      ORDER BY LAST_INJECTION_TIME DESC
    ) STREAMS
    INNER JOIN CMS_STOMGR.STREAM_FILE_QUALITY SFQ
    ON
      SFQ.STREAM = STREAMS.STREAM AND SFQ.RUNNUMBER = STREAMS.RUNNUMBER';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_STREAMS TO PUBLIC';
    END;
  """
  q_view_status_runs="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE VIEW STATUS_RUNS AS
    SELECT * FROM 
    (  
      SELECT
        CMS_STOMGR.STATUS_STREAMS.RUNNUMBER              AS RUNNUMBER,
        SUM(CMS_STOMGR.STATUS_STREAMS.FILES_INJECTED)    AS FILES_INJECTED_RUN,
        SUM(CMS_STOMGR.STATUS_STREAMS.FILES_TRANSFERRED) AS FILES_TRANSFERRED_RUN,
        SUM(CMS_STOMGR.STATUS_STREAMS.FILES_CHECKED)     AS FILES_CHECKED_RUN,
        SUM(CMS_STOMGR.STATUS_STREAMS.FILES_REPACKED)    AS FILES_REPACKED_RUN,
        SUM(CMS_STOMGR.STATUS_STREAMS.FILES_DELETED)     AS FILES_DELETED_RUN,
        SUM(CMS_STOMGR.STATUS_STREAMS.FILES_CORRUPTED)   AS FILES_CORRUPTED_RUN,
        MAX(LAST_INJECTION_TIME)                         AS LAST_INJECTION_TIME_RUN
      FROM CMS_STOMGR.STATUS_STREAMS
      GROUP BY RUNNUMBER
      ORDER BY LAST_INJECTION_TIME_RUN DESC
    ) STREAMS
    INNER JOIN
    (
      SELECT ''Test'' as LABEL, ''Unknown'' as HLT_KEY, ''Unknown'' as CMSSW_VERSION from DUAL
    ) DUMMY_METADATA_TABLE ON 1=1';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_RUNS TO PUBLIC';
    END;
  """
  q_mview_run_file_quality="""
    BEGIN 
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW LOG ON CMS_STOMGR.FILE_QUALITY_CONTROL
    WITH PRIMARY KEY, ROWID  (RUNNUMBER, STREAM, EVENTS_BUILT, EVENTS_LOST, EVENTS_LOST_CHECKSUM, EVENTS_LOST_CMSSW, EVENTS_LOST_CRASH, EVENTS_LOST_OVERSIZED, FILE_SIZE)
    INCLUDING NEW VALUES';
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW RUN_FILE_QUALITY
      CACHE
      BUILD IMMEDIATE
      REFRESH FAST ON COMMIT
    AS SELECT
    RUNNUMBER, 
      COUNT(FILENAME)                   AS NUMBER_OF_FILES            ,
      SUM(NVL(EVENTS_BUILT,0))          AS TOTAL_EVENTS_BUILT         , 
      SUM(NVL(EVENTS_LOST,0))           AS TOTAL_EVENTS_LOST          , 
      SUM(NVL(EVENTS_LOST_CHECKSUM,0))  AS TOTAL_EVENTS_LOST_CHECKSUM , 
      SUM(NVL(EVENTS_LOST_CMSSW,0))     AS TOTAL_EVENTS_LOST_CMSSW    , 
      SUM(NVL(EVENTS_LOST_CRASH,0))     AS TOTAL_EVENTS_LOST_CRASH    , 
      SUM(NVL(EVENTS_LOST_OVERSIZED,0)) AS TOTAL_EVENTS_LOST_OVERSIZED, 
      SUM(NVL(FILE_SIZE,0))             AS TOTAL_SIZE                 , 
      COUNT(NVL(EVENTS_BUILT,0))          , 
      COUNT(NVL(EVENTS_LOST,0))           , 
      COUNT(NVL(EVENTS_LOST_CHECKSUM,0))  , 
      COUNT(NVL(EVENTS_LOST_CMSSW,0))     , 
      COUNT(NVL(EVENTS_LOST_CRASH,0))     , 
      COUNT(NVL(EVENTS_LOST_OVERSIZED,0)) , 
      COUNT(NVL(FILE_SIZE,0))             ,
      COUNT(*)
    FROM CMS_STOMGR.FILE_QUALITY_CONTROL WHERE RUNNUMBER<999999999 AND EVENTS_BUILT>=0 
    GROUP BY RUNNUMBER
    ORDER BY RUNNUMBER DESC'; 
    EXECUTE IMMEDIATE 'GRANT SELECT ON RUN_FILE_QUALITY TO PUBLIC'; 
    END;
  """
  q_mview_stream_file_quality="""
    BEGIN 
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW STREAM_FILE_QUALITY
      CACHE
      BUILD IMMEDIATE
      REFRESH FAST ON COMMIT
    AS SELECT
      RUNNUMBER, 
      STREAM,
      COUNT(FILENAME)                   AS NUMBER_OF_FILES            ,
      SUM(NVL(EVENTS_BUILT,0))          AS TOTAL_EVENTS_BUILT         , 
      SUM(NVL(EVENTS_LOST,0))           AS TOTAL_EVENTS_LOST          , 
      SUM(NVL(EVENTS_LOST_CHECKSUM,0))  AS TOTAL_EVENTS_LOST_CHECKSUM , 
      SUM(NVL(EVENTS_LOST_CMSSW,0))     AS TOTAL_EVENTS_LOST_CMSSW    , 
      SUM(NVL(EVENTS_LOST_CRASH,0))     AS TOTAL_EVENTS_LOST_CRASH    , 
      SUM(NVL(EVENTS_LOST_OVERSIZED,0)) AS TOTAL_EVENTS_LOST_OVERSIZED, 
      SUM(NVL(FILE_SIZE,0))             AS TOTAL_SIZE                 , 
      COUNT(NVL(EVENTS_BUILT,0))          , 
      COUNT(NVL(EVENTS_LOST,0))           , 
      COUNT(NVL(EVENTS_LOST_CHECKSUM,0))  , 
      COUNT(NVL(EVENTS_LOST_CMSSW,0))     , 
      COUNT(NVL(EVENTS_LOST_CRASH,0))     , 
      COUNT(NVL(EVENTS_LOST_OVERSIZED,0)) , 
      COUNT(NVL(FILE_SIZE,0))            ,
      COUNT(*)
    FROM CMS_STOMGR.FILE_QUALITY_CONTROL WHERE RUNNUMBER<999999999 AND EVENTS_BUILT>=0 
    GROUP BY RUNNUMBER, STREAM
    ORDER BY RUNNUMBER DESC, STREAM ASC'; 
    EXECUTE IMMEDIATE 'GRANT SELECT ON STREAM_FILE_QUALITY TO PUBLIC'; 
    END;
  """

  databaseAgent.runQuery('CMS_STOMGR', q_mview_status_streams);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()
  databaseAgent.runQuery('CMS_STOMGR', q_mview_run_file_quality);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()
  databaseAgent.runQuery('CMS_STOMGR', q_mview_stream_file_quality);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()
  databaseAgent.runQuery('CMS_STOMGR', q_view_status_runs);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()
  databaseAgent.runQuery('CMS_STOMGR', q_view_status_streams);
  databaseAgent.cxn_db['CMS_STOMGR'].commit()

def makeViewsLive():
  # views that work in the production environment
  # status streams currently only the last 5 runs, for performance reasons
  q_mview_status_streams="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW MVIEW_STATUS_STREAMS 
      REFRESH COMPLETE START WITH (SYSDATE) NEXT (SYSDATE + 2/1440.)
    AS
    WITH RUNNUMBER_CUTOFF AS (SELECT MIN(RUNNUMBER) FROM (SELECT RUNNUMBER FROM CMS_STOMGR.RUNS WHERE RUNNUMBER<999999999 ORDER BY RUNNUMBER DESC) WHERE ROWNUM<=5)
    SELECT
      FILE_TRANSFER_STATUS.RUNNUMBER                        AS RUNNUMBER,
      FILE_TRANSFER_STATUS.STREAM                           AS STREAM,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,1,1,0))   AS FILES_INJECTED,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,2,1,0))   AS FILES_TRANSFERRED,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,3,1,0))   AS FILES_CHECKED,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,4,1,0))   AS FILES_REPACKED,
      COUNT(*)                                              AS FILES_TOTAL,
      SUM(FILE_TRANSFER_STATUS.DELETED_FLAG)                AS FILES_DELETED,
      SUM(FILE_TRANSFER_STATUS.BAD_CHECKSUM)                AS FILES_CORRUPTED,
      MAX(FILE_TRANSFER_STATUS.P5_INJECTED_TIME)            AS LAST_INJECTION_TIME,
      ROUND(CAST(SUM(FILE_QUALITY_CONTROL.FILE_SIZE) AS FLOAT)/1073741824.,2)
                                                            AS TOTAL_SIZE,
      SUM(FILE_QUALITY_CONTROL.EVENTS_BUILT)                AS TOTAL_EVENTS_BUILT
    FROM CMS_STOMGR.FILE_TRANSFER_STATUS
    INNER JOIN CMS_STOMGR.FILE_QUALITY_CONTROL ON FILE_TRANSFER_STATUS.FILENAME = FILE_QUALITY_CONTROL.FILENAME
    WHERE
      FILE_TRANSFER_STATUS.RUNNUMBER >= (SELECT * FROM RUNNUMBER_CUTOFF) AND
      FILE_TRANSFER_STATUS.RUNNUMBER<999999999
    GROUP BY FILE_TRANSFER_STATUS.RUNNUMBER, FILE_TRANSFER_STATUS.STREAM
    ORDER BY LAST_INJECTION_TIME DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_STREAMS TO PUBLIC';
    END;
  """
  q_mview_status_runs="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW MVIEW_STATUS_RUNS
      REFRESH COMPLETE START WITH (SYSDATE) NEXT (SYSDATE + 2/1440.)
    AS
    WITH RUNNUMBER_CUTOFF AS (SELECT MIN(RUNNUMBER) FROM (SELECT RUNNUMBER FROM CMS_STOMGR.RUNS WHERE RUNNUMBER<999999999 ORDER BY RUNNUMBER DESC) WHERE ROWNUM<=1000),
         RSP_LIMITED AS (
           SELECT RUNNUMBER, NAME, STRING_VALUE FROM CMS_RUNINFO.RUNSESSION_PARAMETER
           WHERE
             RUNNUMBER >= (SELECT * FROM RUNNUMBER_CUTOFF) AND
             (NAME=''CMS.DAQ:CMSSW_VERSION'' OR NAME=''CMS.DAQ:HLT_KEY'')
         )
    SELECT
      FILE_STATUS_RUNS.RUNNUMBER               AS RUNNUMBER,
      FILE_STATUS_RUNS.FILES_INJECTED_RUN      AS FILES_INJECTED_RUN,
      FILE_STATUS_RUNS.FILES_TRANSFERRED_RUN   AS FILES_TRANSFERRED_RUN,
      FILE_STATUS_RUNS.FILES_CHECKED_RUN       AS FILES_CHECKED_RUN,
      FILE_STATUS_RUNS.FILES_REPACKED_RUN      AS FILES_REPACKED_RUN,
      FILE_STATUS_RUNS.FILES_TOTAL_RUN         AS FILES_TOTAL_RUN,
      FILE_STATUS_RUNS.FILES_DELETED_RUN       AS FILES_DELETED_RUN,
      FILE_STATUS_RUNS.FILES_CORRUPTED_RUN     AS FILES_CORRUPTED_RUN,
      FILE_STATUS_RUNS.LAST_INJECTION_TIME_RUN AS LAST_INJECTION_TIME_RUN,
      RSP_CMSSW_VERSION.STRING_VALUE           AS CMSSW_VERSION,
      RSP_HLT_KEY.STRING_VALUE                 AS HLT_KEY,
      RUNS.SETUPLABEL                          AS LABEL
    FROM ( 
      SELECT
        RUNNUMBER,
        SUM(DECODE(STATUS_FLAG,1,1,0))   AS FILES_INJECTED_RUN,
        SUM(DECODE(STATUS_FLAG,2,1,0))   AS FILES_TRANSFERRED_RUN,
        SUM(DECODE(STATUS_FLAG,3,1,0))   AS FILES_CHECKED_RUN,
        SUM(DECODE(STATUS_FLAG,4,1,0))   AS FILES_REPACKED_RUN,
        COUNT(*)                         AS FILES_TOTAL_RUN,
        SUM(DELETED_FLAG)                AS FILES_DELETED_RUN,
        SUM(BAD_CHECKSUM)                AS FILES_CORRUPTED_RUN,
        MAX(P5_INJECTED_TIME)            AS LAST_INJECTION_TIME_RUN
      FROM CMS_STOMGR.FILE_TRANSFER_STATUS WHERE 
        RUNNUMBER >= (SELECT * FROM RUNNUMBER_CUTOFF)
        AND RUNNUMBER<999999999
      GROUP BY RUNNUMBER
    ) FILE_STATUS_RUNS
    INNER JOIN RSP_LIMITED RSP_CMSSW_VERSION
      ON RSP_CMSSW_VERSION.RUNNUMBER = FILE_STATUS_RUNS.RUNNUMBER AND RSP_CMSSW_VERSION.NAME=''CMS.DAQ:CMSSW_VERSION''
    INNER JOIN RSP_LIMITED RSP_HLT_KEY
      ON RSP_HLT_KEY.RUNNUMBER = FILE_STATUS_RUNS.RUNNUMBER AND RSP_HLT_KEY.NAME=''CMS.DAQ:HLT_KEY''
    INNER JOIN CMS_STOMGR.RUNS
      ON RUNS.RUNNUMBER = FILE_STATUS_RUNS.RUNNUMBER
    ORDER BY FILE_STATUS_RUNS.LAST_INJECTION_TIME_RUN DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_RUNS TO PUBLIC';
    END;
  """
  q_mview_run_file_quality="""
    BEGIN 
    EXECUTE IMMEDIATE 'CREATE MATERIALIZED VIEW MVIEW_RUN_FILE_QUALITY 
      REFRESH COMPLETE START WITH (SYSDATE) NEXT (SYSDATE + 2/1440.)
    AS
    WITH RUNNUMBER_CUTOFF AS (SELECT MIN(RUNNUMBER) FROM (SELECT RUNNUMBER FROM CMS_STOMGR.RUNS WHERE RUNNUMBER<999999999 ORDER BY RUNNUMBER DESC) WHERE ROWNUM<=1000)
    SELECT
      RUNNUMBER,
      COUNT(FILENAME)                      AS NUMBER_OF_FILES            ,
      SUM(NVL(EVENTS_BUILT,0))             AS TOTAL_EVENTS_BUILT         ,
      SUM(NVL(EVENTS_LOST,0))              AS TOTAL_EVENTS_LOST          ,
      SUM(NVL(EVENTS_LOST_CHECKSUM,0))     AS TOTAL_EVENTS_LOST_CHECKSUM ,
      SUM(NVL(EVENTS_LOST_CMSSW,0))        AS TOTAL_EVENTS_LOST_CMSSW    ,
      SUM(NVL(EVENTS_LOST_CRASH,0))        AS TOTAL_EVENTS_LOST_CRASH    ,
      SUM(NVL(EVENTS_LOST_OVERSIZED,0))    AS TOTAL_EVENTS_LOST_OVERSIZED,
      ROUND(CAST(SUM(NVL(FILE_SIZE,0)) AS FLOAT)/1073741824.,2)
                                           AS TOTAL_SIZE                 ,
      SUM(NVL(EVENTS_ACCEPTED,0))          AS TOTAL_EVENTS_ACCEPTED      
    FROM CMS_STOMGR.FILE_QUALITY_CONTROL
    WHERE
      RUNNUMBER >= (SELECT * FROM RUNNUMBER_CUTOFF) AND
      RUNNUMBER<999999999 AND
      EVENTS_BUILT>0
    GROUP BY RUNNUMBER ORDER BY RUNNUMBER DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON RUN_FILE_QUALITY TO PUBLIC';
    END;
  """
  q_views="""
    BEGIN
    EXECUTE IMMEDIATE 'CREATE VIEW RUN_FILE_QUALITY AS SELECT * FROM MVIEW_RUN_FILE_QUALITY ORDER BY RUNNUMBER DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON RUN_FILE_QUALITY TO PUBLIC';
    EXECUTE IMMEDIATE 'CREATE VIEW STATUS_RUNS AS SELECT * FROM MVIEW_STATUS_RUNS ORDER BY LAST_INJECTION_TIME_RUN DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_RUNS TO PUBLIC';
    EXECUTE IMMEDIATE 'CREATE VIEW STATUS_STREAMS AS SELECT * FROM MVIEW_STATUS_STREAMS ORDER BY RUNNUMBER DESC';
    EXECUTE IMMEDIATE 'GRANT SELECT ON STATUS_STREAMS TO PUBLIC';
    END;
  """
  t1=time.time()
  databaseAgent.runQuery('CMS_STOMGR', q_mview_run_file_quality)
  print("q_mview_run_file_quality done")
  databaseAgent.runQuery('CMS_STOMGR', q_mview_status_runs)
  print("q_mview_status_runs done")
  databaseAgent.runQuery('CMS_STOMGR', q_mview_status_streams)
  print("q_mview_status_streams done")
  databaseAgent.cxn_db['CMS_STOMGR'].commit()
  print("mviews created, took {0} seconds".format(time.time()-t1))
  databaseAgent.runQuery('CMS_STOMGR', q_views)
  databaseAgent.cxn_db['CMS_STOMGR'].commit()

def refreshViews():
    cursor=databaseAgent.cxn_db['CMS_STOMGR'].cursor()
    t1=time.time()
    cursor.callproc("DBMS_MVIEW.REFRESH", ['MVIEW_STATUS_RUNS','C','',True, False,0,0,0,True,False])
    t2=time.time()
    print "refreshed MVIEW_STATUS_RUNS in ", t2-t1, "s"
    t1=time.time()
    cursor.callproc("DBMS_MVIEW.REFRESH", ['MVIEW_STATUS_STREAMS','C','',True, False,0,0,0,True,False])
    t2=time.time()
    print "refreshed MVIEW_STATUS_STREAMS in ", t2-t1, "s"
    t1=time.time()
    cursor.callproc("DBMS_MVIEW.REFRESH", ['MVIEW_RUN_FILE_QUALITY','C','',True, False,0,0,0,True,False])
    t2=time.time()
    print "refreshed MVIEW_RUN_FILE_QUALITY in ", t2-t1, "s"

def dropViews():
    databaseAgent.runQuery('CMS_STOMGR', "DROP MATERIALIZED VIEW MVIEW_STATUS_RUNS", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP MATERIALIZED VIEW MVIEW_STATUS_STREAMS", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP MATERIALIZED VIEW MVIEW_RUN_FILE_QUALITY", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP VIEW RUN_FILE_QUALITY", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP VIEW STATUS_RUNS", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP VIEW STATUS_STREAMS", False)
    databaseAgent.cxn_db['CMS_STOMGR'].commit()

# Don't run this in the production environment, ever.
def dropTestTables():
    databaseAgent.runQuery('CMS_STOMGR', "DROP SEQUENCE FILE_ID_SEQ", False)
    databaseAgent.runQuery('CMS_STOMGR', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'FILE_TRANSFER_STATUS'; if existing_tables > 0 then execute immediate 'drop table FILE_TRANSFER_STATUS'; end if; end;", False)
    databaseAgent.runQuery('CMS_STOMGR', "DROP FUNCTION T0_NEEDS_TO_INJECT")
    databaseAgent.cxn_db['CMS_STOMGR'].commit()

def testViewQueries():
  query_status_streams="""
    SELECT
      FILE_TRANSFER_STATUS.RUNNUMBER                        AS RUNNUMBER,
      FILE_TRANSFER_STATUS.STREAM                           AS STREAM,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,1,1,0))   AS FILES_INJECTED,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,2,1,0))   AS FILES_TRANSFERRED,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,3,1,0))   AS FILES_CHECKED,
      SUM(DECODE(FILE_TRANSFER_STATUS.STATUS_FLAG,4,1,0))   AS FILES_REPACKED,
      COUNT(*)                                              AS FILES_TOTAL,
      SUM(FILE_TRANSFER_STATUS.DELETED_FLAG)                AS FILES_DELETED,
      SUM(FILE_TRANSFER_STATUS.BAD_CHECKSUM)                AS FILES_CORRUPTED,
      MAX(FILE_TRANSFER_STATUS.P5_INJECTED_TIME)            AS LAST_INJECTION_TIME,
      SUM(FILE_QUALITY_CONTROL.FILE_SIZE)                   AS TOTAL_FILE_SIZE,
      SUM(FILE_QUALITY_CONTROL.EVENTS_BUILT)                AS TOTAL_EVENTS_BUILT
    FROM CMS_STOMGR.FILE_TRANSFER_STATUS
    INNER JOIN CMS_STOMGR.FILE_QUALITY_CONTROL ON FILE_TRANSFER_STATUS.FILENAME = FILE_QUALITY_CONTROL.FILENAME
    WHERE
      FILE_TRANSFER_STATUS.RUNNUMBER >= (
        SELECT MIN(RUNNUMBER) FROM (SELECT RUNNUMBER FROM CMS_STOMGR.RUNS ORDER BY RUNNUMBER DESC) WHERE ROWNUM<=100
      ) AND
      FILE_TRANSFER_STATUS.RUNNUMBER<999999999 AND
      FILE_QUALITY_CONTROL.EVENTS_BUILT>0
    GROUP BY FILE_TRANSFER_STATUS.RUNNUMBER, FILE_TRANSFER_STATUS.STREAM
    ORDER BY LAST_INJECTION_TIME DESC
  """
  query_status_runs="""
    SELECT
      FILE_STATUS_RUNS.RUNNUMBER                            AS RUNNUMBER,
      FILE_STATUS_RUNS.FILES_INJECTED_RUN                   AS FILES_INJECTED_RUN,
      FILE_STATUS_RUNS.FILES_TRANSFERRED_RUN                AS FILES_TRANSFERRED_RUN,
      FILE_STATUS_RUNS.FILES_CHECKED_RUN                    AS FILES_CHECKED_RUN,
      FILE_STATUS_RUNS.FILES_REPACKED_RUN                   AS FILES_REPACKED_RUN,
      FILE_STATUS_RUNS.FILES_DELETED_RUN                    AS FILES_DELETED_RUN,
      FILE_STATUS_RUNS.FILES_CORRUPTED_RUN                  AS FILES_CORRUPTED_RUN,
      FILE_STATUS_RUNS.LAST_INJECTION_TIME_RUN              AS LAST_INJECTION_TIME_RUN,
      RSP_CMSSW_VERSION.STRING_VALUE                        AS CMSSW_VERSION,
      RSP_HLT_KEY.STRING_VALUE                              AS HLT_KEY,
      RUNS.SETUPLABEL                                       AS LABEL
    FROM ( 
      SELECT
        RUNNUMBER,
        SUM(DECODE(STATUS_FLAG,1,1,0))   AS FILES_INJECTED_RUN,
        SUM(DECODE(STATUS_FLAG,2,1,0))   AS FILES_TRANSFERRED_RUN,
        SUM(DECODE(STATUS_FLAG,3,1,0))   AS FILES_CHECKED_RUN,
        SUM(DECODE(STATUS_FLAG,4,1,0))   AS FILES_REPACKED_RUN,
        COUNT(*)                         AS FILES_TOTAL,
        SUM(DELETED_FLAG)                AS FILES_DELETED_RUN,
        SUM(BAD_CHECKSUM)                AS FILES_CORRUPTED_RUN,
        MAX(P5_INJECTED_TIME)            AS LAST_INJECTION_TIME_RUN
      FROM CMS_STOMGR.FILE_TRANSFER_STATUS WHERE 
        RUNNUMBER >= (
          SELECT MIN(RUNNUMBER) FROM (SELECT RUNNUMBER FROM CMS_STOMGR.RUNS ORDER BY RUNNUMBER DESC) WHERE ROWNUM<=100
        ) AND
        RUNNUMBER<999999999
      GROUP BY RUNNUMBER
    ) FILE_STATUS_RUNS
    INNER JOIN CMS_RUNINFO.RUNSESSION_PARAMETER RSP_CMSSW_VERSION
      ON RSP_CMSSW_VERSION.RUNNUMBER = FILE_STATUS_RUNS.RUNNUMBER AND RSP_CMSSW_VERSION.NAME='CMS.DAQ:CMSSW_VERSION'
    INNER JOIN CMS_RUNINFO.RUNSESSION_PARAMETER RSP_HLT_KEY
      ON RSP_HLT_KEY.RUNNUMBER = FILE_STATUS_RUNS.RUNNUMBER AND RSP_HLT_KEY.NAME='CMS.DAQ:HLT_KEY'
    INNER JOIN CMS_STOMGR.RUNS
      ON RUNS.RUNNUMBER = FILE_STATUS_RUNS.RUNNUMBER
    ORDER BY FILE_STATUS_RUNS.LAST_INJECTION_TIME_RUN DESC
  """
  query_run_file_quality="""
    SELECT
      RUNNUMBER,
      COUNT(FILENAME)                   AS NUMBER_OF_FILES            ,
      SUM(NVL(EVENTS_BUILT,0))          AS TOTAL_EVENTS_BUILT         ,
      SUM(NVL(EVENTS_LOST,0))           AS TOTAL_EVENTS_LOST          ,
      SUM(NVL(EVENTS_LOST_CHECKSUM,0))  AS TOTAL_EVENTS_LOST_CHECKSUM ,
      SUM(NVL(EVENTS_LOST_CMSSW,0))     AS TOTAL_EVENTS_LOST_CMSSW    ,
      SUM(NVL(EVENTS_LOST_CRASH,0))     AS TOTAL_EVENTS_LOST_CRASH    ,
      SUM(NVL(EVENTS_LOST_OVERSIZED,0)) AS TOTAL_EVENTS_LOST_OVERSIZED,
      SUM(NVL(FILE_SIZE,0))             AS TOTAL_SIZE
    FROM CMS_STOMGR.FILE_QUALITY_CONTROL
    WHERE
      RUNNUMBER >= (SELECT MIN(RUNNUMBER) FROM (SELECT * FROM CMS_STOMGR.RUNS ORDER BY RUNNUMBER DESC) WHERE ROWNUM<=100) AND
      RUNNUMBER<999999999 AND
      EVENTS_BUILT>=0
    GROUP BY RUNNUMBER ORDER BY RUNNUMBER DESC
  """
  query_stream_file_quality="""
    SELECT
      RUNNUMBER,
      STREAM,
      COUNT(FILENAME)                   AS NUMBER_OF_FILES            ,
      SUM(NVL(EVENTS_BUILT,0))          AS TOTAL_EVENTS_BUILT         ,
      SUM(NVL(EVENTS_LOST,0))           AS TOTAL_EVENTS_LOST          ,
      SUM(NVL(EVENTS_LOST_CHECKSUM,0))  AS TOTAL_EVENTS_LOST_CHECKSUM ,
      SUM(NVL(EVENTS_LOST_CMSSW,0))     AS TOTAL_EVENTS_LOST_CMSSW    ,
      SUM(NVL(EVENTS_LOST_CRASH,0))     AS TOTAL_EVENTS_LOST_CRASH    ,
      SUM(NVL(EVENTS_LOST_OVERSIZED,0)) AS TOTAL_EVENTS_LOST_OVERSIZED,
      SUM(NVL(FILE_SIZE,0))             AS TOTAL_SIZE
    FROM CMS_STOMGR.FILE_QUALITY_CONTROL
    WHERE
      RUNNUMBER >= (SELECT MIN(RUNNUMBER) FROM (SELECT * FROM CMS_STOMGR.RUNS ORDER BY RUNNUMBER DESC) WHERE ROWNUM<=100) AND
      RUNNUMBER<999999999 AND
      EVENTS_BUILT>=0
    GROUP BY RUNNUMBER, STREAM
    ORDER BY RUNNUMBER DESC, STREAM ASC
  """
  queries_to_test={"status streams": query_status_streams, "status runs": query_status_runs, "run file quality": query_run_file_quality, "stream file quality": query_stream_file_quality}
  for query in queries_to_test:
    print 'testing', query, 'query'
    for i in range(100):
      t1=time.time()
      databaseAgent.runQuery('file_status', queries_to_test[query], True);
      t2=time.time()
      print i+1, t2-t1


