#!/bin/env python

# Last modified by Dylan G. Hsu on 2015-05-29 :: dylan.hsu@cern.ch

import os,sys,socket
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
    query="UPDATE FILE_TRANSFER_STATUS SET STATUS_FLAG=255 WHERE STATUS_FLAG<>255"
    databaseAgent.runQuery('hlt_rates_write',query,False)
    databaseAgent.cxn_db['hlt_rates_write'].commit()

def testPolling(flag):
    n=1000
    distr_file = open("/nfshome0/dhsu/polling_times.dat", "w")
    for i in range(1,1000):
        markAllRowsCompleted()
        markRandomRows(n)
        query="SELECT FILENAME FROM FILE_TRANSFER_STATUS WHERE BITAND(STATUS_FLAG,{0})={0}".format(flag)
        t1=int(round(time.time() * 1000000))/1000.
        result=databaseAgent.runQuery('hlt_rates_write',query,True)
        num_found=len( result )
        delta_t = int(round(time.time() * 1000000))/1000. - t1
        print "took {0} ms to poll and retrieve {1} filenames".format(delta_t, num_found)
        distr_file.write("{0} {1} {2}\n".format(i, num_found, delta_t))
    markAllRowsCompleted()
    distr_file.close()

def populateFtsTable():
    stream="DGH"
    time0=int(round(time.time() * 1000))
    for runnumber in range(100001,200000):
        print "inserting for runnumber {0}".format(runnumber)
        query="INSERT ALL "
        for lumisection in range(0,999):
            filename="run{0}_ls{1}_stream{2}_dvmrg-c2f37-21-01.dat".format(runnumber,lumisection,stream)
            query+=("INTO FILE_TRANSFER_STATUS (RUNNUMBER,LS,STREAM,FILENAME,LAST_UPDATE_TIME,STATUS_FLAG) "+
              "VALUES ({0},{1},'{2}','{3}',{4},{5}) ").format(
                runnumber,
                lumisection,
                stream,
                filename,
                "TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')",
                255
              )
        query+="SELECT 1 FROM DUAL"
        time_this_runnumber=int(round(time.time() * 1000))
        databaseAgent.runQuery('hlt_rates_write',query, False)
        databaseAgent.cxn_db['hlt_rates_write'].commit()
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
        query="UPDATE FILE_TRANSFER_STATUS SET STATUS_FLAG=(255-BITAND(255-STATUS_FLAG,255-{0})) WHERE FILENAME='{1}'".format(status_flag, filename)
        time_this_lumisection=int(round(time.time() * 1000000))/1000.
        databaseAgent.runQuery('hlt_rates_write',query,False)
        databaseAgent.cxn_db['hlt_rates_write'].commit()
        delta_t = int(round(time.time() * 1000000))/1000. - time_this_lumisection
        total_time+=delta_t
        query="SELECT 1 FROM DUAL"
        time_control_query=int(round(time.time() * 1000000))/1000.
        databaseAgent.runQuery('hlt_rates_write',query,False)
        delta_t_control=int(round(time.time() * 1000000))/1000. - time_control_query
        print ". . . took {0} ms (dummy query took {1} ms)".format(delta_t, delta_t_control)
        distr_file.write("{0} {1} {2} {3}\n".format(runnumber, lumisection, delta_t, delta_t_control))
    print "sum of all update query times: {0} ms".format(total_time)
    distr_file.close()

# This method is used for testing only and should not be run in the production environment, ever!
# Also serves as internal documentation of the necessary tables because this stuff is not written down anywhere...
def makeTestTables():
#      STATUS_FLAG ENUM('P5_OPENED', 'P5_CLOSED', 'T0_INJECTED', 'TRANSFERRED', 'T0_CHECKED', 'T0_REPACKED', 'P5_DELETED'),
    q_table1="CREATE TABLE FILE_STATUS_FLAGS ("+\
      "STATUS_FLAG NUMBER(3) NOT NULL, "+\
      "MEANING VARCHAR2(16) NOT NULL, "+\
      "PRIMARY KEY (STATUS_FLAG) "+\
    ")"
      
    q_table2="INSERT ALL "+\
      "INTO FILE_STATUS_FLAGS (STATUS_FLAG, MEANING) VALUES (1,  'P5_OPENED') "+\
      "INTO FILE_STATUS_FLAGS (STATUS_FLAG, MEANING) VALUES (2,  'P5_CLOSED') "+\
      "INTO FILE_STATUS_FLAGS (STATUS_FLAG, MEANING) VALUES (4,  'T0_INJECTED') "+\
      "INTO FILE_STATUS_FLAGS (STATUS_FLAG, MEANING) VALUES (8,  'TRANSFERRED') "+\
      "INTO FILE_STATUS_FLAGS (STATUS_FLAG, MEANING) VALUES (16, 'T0_CHECKED') "+\
      "INTO FILE_STATUS_FLAGS (STATUS_FLAG, MEANING) VALUES (32, 'T0_REPACKED') "+\
      "INTO FILE_STATUS_FLAGS (STATUS_FLAG, MEANING) VALUES (64, 'P5_DELETED') "+\
      "SELECT 1 FROM DUAL"

    q_table3="BEGIN "+\
      "EXECUTE IMMEDIATE 'CREATE TABLE FILE_TRANSFER_STATUS ( "+\
        "RUNNUMBER          NUMBER(22)     NOT NULL, "+\
        "LS                 NUMBER(22)     NOT NULL, "+\
        "STREAM             VARCHAR2(256)  NOT NULL, "+\
        "FILENAME           VARCHAR2(1000) NOT NULL, "+\
        "CHECKSUM           VARCHAR2(50), "+\
        "LAST_UPDATE_TIME   TIMESTAMP(6), "+\
        "STATUS_FLAG NUMBER(3) NOT NULL, "+\
        "PRIMARY KEY (FILENAME), "+\
        "CONSTRAINT CONS_FTS_LUMIS UNIQUE(RUNNUMBER, LS, STREAM) "+\
      ")'; "+\
      "EXECUTE IMMEDIATE 'CREATE INDEX IDX_FTS_FLAGS "+\
        "ON FILE_TRANSFER_STATUS ( "+\
          "STATUS_FLAG "+\
        ")'; "+\
    "END;"
#        #"FOREIGN KEY (STATUS_FLAG) REFERENCES FILE_STATUS_FLAGS(STATUS_FLAG), "+\
    try:
        query=q_table1
        databaseAgent.runQuery('hlt_rates_write', q_table1, False)
        query=q_table2
        databaseAgent.runQuery('hlt_rates_write', q_table2, False)
        databaseAgent.cxn_db['hlt_rates_write'].commit()
        query=q_table3
        databaseAgent.runQuery('hlt_rates_write', q_table3, False)
        databaseAgent.cxn_db['hlt_rates_write'].commit()
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print error.code
        print error.message
        print error.context
        print error.offset
        print query[max(0,error.offset-20):min(len(query)-1,error.offset+20)]


# Don't run this in the production environment, ever.
def dropTestTables():
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'FILE_TRANSFER_STATUS'; if existing_tables > 0 then execute immediate 'drop table FILE_TRANSFER_STATUS'; end if; end;", False)
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'FILE_STATUS_FLAGS'; if existing_tables > 0 then execute immediate 'drop table FILE_STATUS_FLAGS'; end if; end;", False)
    databaseAgent.cxn_db['hlt_rates_write'].commit()
