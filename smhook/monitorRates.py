#!/bin/env python

# Last modified by Dylan G. Hsu on 2016-11-25 :: dylan.hsu@cern.ch

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json
import logging

import smhook.config
import smhook.databaseAgent as databaseAgent

debug=False
logger = logging.getLogger(__name__)
# For debugging purposes, initialize the logger to stdout if running script as a standalone
if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

#############################
# monitorRates:             #
#############################
#
# Supply this method with a FULL PATH to a .jsndata file to read it and put the HLT or L1 rates inside into the database.
#         As of May 2015 we also put the dataset acceptances in another table for the HLT
#        and store the L1 rates type by type.

# The jsndata needs the .ini descriptor files to exist in relative subdirectory "./open" or this will fail!
#
# For the HLT rates, each lumisection has several path IDs which get their own row for that LS.
# We get these path IDs by looking up the path names provided in the .ini file in a mapping
# from the production database.
# For the L1 rates, we insert a single row per LS, per type (algo|technical)x(all|physics|calibration|random)
# However in this row are two Varrays containing 128 (64) bits for the algo (technical) for the trigger rates,
# accomplishing in 1 row what the HLT rates table does with many rows.

def monitorRates(jsndata_file,rates_jsn_file):
    # This takes the full path of a .jsndata file as parameter
    # Any other call of this function is inappropriate and will just not work!
    # e.g. jsndata_file='/store/lustre/mergeMiniDAQMacro/run230852/run230852_ls0110_streamHLTRates_mrg-c2f13-37-01.jsndata'

    # Grab the globals for the database connections
    global cxn_exists, cxn_db, cursor, cxn_timestamp
    is_fresh_cxn={}

    # Do some filename handling to get info about the run, stream, LS
    jsn_file = rates_jsn_file
    json_dir=os.path.dirname(jsndata_file) 
    jsndata_filename=os.path.basename(jsndata_file)
    file_raw, file_ext = os.path.splitext(jsndata_filename)
    raw_pieces=file_raw.split( '_' , 3 ) # this is not an emoji!
    run_number=raw_pieces[0][3:] # 123456
    ls=raw_pieces[1] # ls1234
    stream=raw_pieces[2][6:] # HLTRates | L1Rates
    machine=raw_pieces[3]

    if stream != "HLTRates" and stream != "L1Rates":
        logger.error('Unrecognized rate stream: '+raw_pieces[2])
        return False
    
    # We only need the trigger tables and dataset name-ID mapping for the HLT rates:
    if stream=="HLTRates":
        # Retrieve the mapping between HLT path index, HLT path name, HLT path ID
        pathname_query="""
            select D.ORD, A.NAME, E.PATHID from
                CMS_HLT_GDR.U_PATHS A,
                CMS_HLT_GDR.U_PATHIDS E,
                CMS_HLT_GDR.U_PATHID2CONF D,
                CMS_WBM.RUNSUMMARY C,
                CMS_HLT_GDR.U_CONFVERSIONS B 
            where 
                A.ID = E.ID_PATH
                and E.ID = D.ID_PATHID
                and D.ID_CONFVER = B.ID
                and B.CONFIGID = C.HLTKEY
                and C.RUNNUMBER = {0}
            order by D.ORD
        """.format(str(run_number))
        retries=0
        pathname_mapping=databaseAgent.runQuery('trigger_read', pathname_query)
        if pathname_mapping==False or len(pathname_mapping) < 1:
            logger.error("Could not get pathname-pathID mapping for HLT rates!")
            return False

        # Retrieve the mapping between dataset name and dataset ID
        dataset_name_query="""
            select distinct E.NAME , D.ID
            from CMS_HLT_GDR.U_CONFVERSIONS A, CMS_HLT_GDR.U_CONF2STRDST B, CMS_WBM.RUNSUMMARY C, CMS_HLT_GDR.U_DATASETIDS D, CMS_HLT_GDR.U_DATASETS E 
            WHERE
                D.ID=B.ID_DATASETID 
                and E.ID=D.ID_DATASET
                and B.ID_CONFVER=A.ID
                and A.CONFIGID = C.HLTKEY 
                and C.RUNNUMBER = {0} order by E.name
        """.format(str(run_number))
        dataset_name_mapping=databaseAgent.runQuery('trigger_read', dataset_name_query)
        if dataset_name_mapping==False or len(dataset_name_mapping) < 1:
            logger.error("Could not get pathname-pathID mapping for HLT rates!")
            return False

    # Open the jsndata file
    # If it doesn't exist, this function will crash
    try:
        rates_json=open(jsndata_file).read()
    except (OSError, IOError) as e:
        logger.error('Error finding or opening jsndata file: "'+jsndata_file+'"')
        return False
    try:
        rates=json.loads(rates_json)
    except (ValueError) as e:
        #check json
        settings_textI = open(jsn_file).read()
        settings = json.loads(settings_textI)
        #check the eventsNumber
        if int(settings['data'][1]) == 0:
            return
        else:
            logger.error('The jsndata is not readable but you gave me a json with events!')
            return False

    # Get the ini in the directory with rates corresponding to specified run number, lumi section, and HLT|L1
    # The correspondence might look like:
    # run230852_ls0110_streamL1Rates_mrg-c2f13-37-01.jsndata =>
    # run230852_ls0000_streamL1Rates_mrg-c2f13-37-01.ini
    # If the INI file is not there, this function will crash

    ini_filename=raw_pieces[0]+'_ls0000_'+raw_pieces[2]+'_StorageManager.ini'
    ini_path = os.path.join(json_dir, 'open', ini_filename)
    if stream=='HLTRates':
        try:
            HLT_json=open(ini_path).read()
        except (OSError, IOError) as e:
            logger.error("Error finding or opening ini file: `%s'" % ini_path)
            logger.exception(e)
            return False
        HLT_names=json.loads(HLT_json)
        HLT_rates={}
        i=0
        # Get the rates for each trigger path
        HLT_LS_info={}
        HLT_LS_info['PROC']=0 # number of events processed
        for pathname in HLT_names['Path-Names']:
            HLT_rates[pathname]={}
            HLT_rates[pathname]['L1PASS']     = rates['data'][2][i]
            HLT_rates[pathname]['PSPASS']     = rates['data'][3][i]
            HLT_rates[pathname]['PACCEPT']     = rates['data'][4][i]
            HLT_rates[pathname]['PREJECT']     = rates['data'][5][i]
            HLT_rates[pathname]['PEXCEPT']     = rates['data'][6][i]
            if HLT_LS_info['PROC']==0:
                HLT_LS_info['PROC']=rates['data'][0][0]
            i+=1
        
        # Handle Dataset acceptances appended at end of json file
        HLT_dataset_acceptances={}
        i=0
        for dataset_name in HLT_names['Dataset-Names']:
            HLT_dataset_acceptances[dataset_name] = rates['data'][7][i]
            i+=1

        # Before we put the rates in the DB, we will need see if the LS is indexed in the DB
        # If it isn't, we create a row in the table of LS
        # Currently most of the fields are set to 0 because I am grossly misinformed
        query="SELECT RUNNUMBER FROM "+HLT_LS_table+" WHERE LSNUMBER="+ls[2:]+" AND RUNNUMBER="+run_number
        result=databaseAgent.runQuery('hlt_rates_write', query)
        if result==False:
            databaseAgent.cxn_db["hlt_rates_write"].rollback()
            return False        
        elif len(result) < 1:
            # No existing row. we must now try to insert:
            # print "This LS is not already in the DB" #debug    
            query="""
                INSERT INTO {0} (
                    RUNNUMBER, LSNUMBER, MODIFICATIONTIME, PSINDEX, PSINDMATCH, PROC, ACC, ENTRIESRCV, ENTRIESEXP, EFFREP, EXPREP,  MODDIFF
                ) VALUES (
                    {1},       {2},      {3},              {4},     {5},        {6},  {7}, {8},        {9},        {10},   {11},    {12}
                )
            """.format(
                HLT_LS_table,
                run_number,
                ls[2:],
                "TO_TIMESTAMP('"+str(datetime.datetime.utcfromtimestamp(os.path.getmtime(jsndata_file)))+"','YYYY-MM-DD HH24:MI:SS.FF6')", #UTC timestamp -> oracle
                0,
                0,
                HLT_LS_info['PROC'],
                0,
                0,
                0,
                0,
                0,
                0
            )
            result=databaseAgent.runQuery('hlt_rates_write', query, False)
            if result==False:
                databaseAgent.cxn_db["hlt_rates_write"].rollback()
                return False
        # End checking/indexing LS in the DB

        # Now put the rates in the DB
        # This is kept separate from the above part because we might want to do something smarter with the path mapping later
        for pathname in HLT_rates:
            path_id=-1
            # Loop over tuples in the mapping to find the path id for this particular path name
            # This is klugey, but it's less memory intensive than using a dict
            for tuple in pathname_mapping:
                if pathname==tuple[1]:
                    path_id=tuple[2]

            if path_id == -1:
                logger.error('Pathname "'+pathname+'" missing from pathname-pathID mapping!')
                return False

            query="""
                INSERT INTO {0} (
                    RUNNUMBER, LSNUMBER, PATHID, L1PASS, PSPASS, PACCEPT, PEXCEPT, PREJECT
                ) VALUES (
                    {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}
                )
            """.format(
                HLT_rates_table,
                run_number,
                int(ls[2:]),
                path_id,
                HLT_rates[pathname]['L1PASS'], 
                HLT_rates[pathname]['PSPASS'],
                HLT_rates[pathname]['PACCEPT'],
                HLT_rates[pathname]['PEXCEPT'],
                HLT_rates[pathname]['PREJECT']
            )
            result=databaseAgent.runQuery('hlt_rates_write', query, False)
            if result==False:
                databaseAgent.cxn_db["hlt_rates_write"].rollback()
                return False
            databaseAgent.cxn_db["hlt_rates_write"].commit()

        # Put the dataset acceptances in the DB using similar method as the rates
        for dataset_name in HLT_dataset_acceptances:
            dataset_id=-1
            # Loop over tuples in the mapping to find the dataset ID for this particular dataset name
            for tuple in dataset_name_mapping:
                if dataset_name==tuple[0]:
                    dataset_id=tuple[1]

            if dataset_id==-1:
                logger.error('Dataset name "'+dataset_name+'" missing from dataset name-ID mapping!')
                return False
            query="""
                INSERT INTO {0} (
                    RUNNUMBER, LSNUMBER, DATASETID, ACCEPT
                ) VALUES (
                    {1},       {2},      {3},       {4}
                )
            """.format(
                HLT_datasets_table,
                run_number,
                int(ls[2:]),
                dataset_id,
                HLT_dataset_acceptances[dataset_name]
            )
            result=databaseAgent.runQuery('hlt_rates_write', query, False)
            if result==False:
                databaseAgent.cxn_db["hlt_rates_write"].rollback()
                return False
            datagbaseAgent.cxn_db["hlt_rates_write"].commit()
        return True
    
    elif stream=='L1Rates':
        try:
            L1_json=open(ini_path).read()
        except (OSError, IOError) as e:
            logger.error("Error finding or opening ini file: `%s'" % ini_path)
            logger.exception(e)
            return False
        
        L1_names=json.loads(L1_json)
        L1_rates={}
        L1_rates['L1_DECISION']             = rates['data'][1]
        L1_rates['L1_DECISION_PHYSICS']     = rates['data'][2]
        L1_rates['L1_DECISION_CALIBRATION'] = rates['data'][3]
        L1_rates['L1_DECISION_RANDOM']      = rates['data'][4]
        L1_rates['mod_datetime']            = str(datetime.datetime.utcfromtimestamp(os.path.getmtime(jsndata_file)))
        
        lumisection_id = "%07d_%05d" % (int(run_number), int(ls[2:]))

        # Retrieve the IDs for the different types of L1 rates from the lookup table
        l1_all_rates_result = databaseAgent.runQuery("l1_rate_types_read", "SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_BY_HLT') FROM DUAL")
        l1_physics_rates_result = databaseAgent.runQuery("l1_rate_types_read", "SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_PHYSICS') FROM DUAL")
        l1_calibration_rates_result = databaseAgent.runQuery("l1_rate_types_read", "SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_CALIBRATION') FROM DUAL")
        l1_random_rates_result = databaseAgent.runQuery("l1_rate_types_read", "SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_RANDOM') FROM DUAL")
        if (l1_all_rates_result==False) or (l1_physics_rates_result==False) or (l1_calibration_rates_result==False) or (l1_random_rates_result==False):
            logger.error("DB error looking up L1 rate ID types")
            return False
        if len(l1_all_rates_result) < 1 or len(l1_physics_rates_result) < 1 or len(l1_calibration_rates_result) < 1 or len(l1_random_rates_result) < 1:
            logger.error("One of the necessary ID's for the L1 rate types does not exist. Someone changed the database!")
            return False
        # Create a dict that we loop over for inserting the 2048 rate numbers in the db 512 at a time
        l1_rate_type_dict = {
          'L1_DECISION'             : l1_all_rates_result[0][0],
          'L1_DECISION_PHYSICS'     : l1_physics_rates_result[0][0],
          'L1_DECISION_CALIBRATION' : l1_calibration_rates_result[0][0],
          'L1_DECISION_RANDOM'      : l1_random_rates_result[0][0]
        }
        for l1_rate_type_name in l1_rate_type_dict:
            scaler_type = l1_rate_type_dict[l1_rate_type_name]
            algo_index=0 #indexing starts from 0
            for algo_count in L1_rates[l1_rate_type_name]:
                algo_rate = algo_count / (3564 * 2**18 / 40078970.0)
                query = "INSERT INTO %s (ALGO_INDEX, ALGO_COUNT, ALGO_RATE, SCALER_TYPE, LUMI_SECTIONS_ID) VALUES ( %d, %d, %f, %d, '%s' )" % (
                    L1_rates_table,
                    algo_index,
                    algo_count,                   
                    algo_rate,
                    scaler_type,
                    lumisection_id
                )
                logger.debug(query)
                result=databaseAgent.runQuery('l1_rates_write', query, False)
                if result==False:
                    databaseAgent.cxn_db["l1_rates_write"].rollback()
                    return False
                algo_index=algo_index+1
        databaseAgent.cxn_db["l1_rates_write"].commit()
        return True

def getMyTables():
    result=databaseAgent.runQuery('hlt_rates_write', "SELECT owner, table_name FROM all_tables WHERE owner='CMS_DAQ_TEST_RUNINFO'")
    print result

# This method is used for testing only and should not be run in the production environment, ever!
# Also serves as internal documentation of the necessary tables because this stuff is not written down anywhere...
def makeTestTables():
    #q_varray1="create or replace type L1_DECISION_VARRAY is VARRAY(128) of NUMBER(11)"
    #q_varray2="create or replace type L1_TECHNICAL_VARRAY is VARRAY(64) of NUMBER(11)"
    q_table1="""create table HLT_TEST_LUMISECTIONS_V3
        (
            RUNNUMBER            NUMBER(11)    NOT NULL, 
            LSNUMBER             NUMBER(11)    NOT NULL, 
            MODIFICATIONTIME     TIMESTAMP(6)  NOT NULL, 
            PSINDEX              NUMBER(11)    NOT NULL, 
            PSINDMATCH           NUMBER(1)     NOT NULL, 
            PROC                 NUMBER(11)    NOT NULL, 
            ACC                  NUMBER(11)    NOT NULL, 
            ENTRIESRCV           NUMBER(11)    NOT NULL, 
            ENTRIESEXP           NUMBER(11)    NOT NULL, 
            EFFREP               NUMBER(11)    NOT NULL, 
            EXPREP               NUMBER(11)    NOT NULL, 
            MODDIFF              NUMBER(11)    NOT NULL,
            PRIMARY KEY (LSNUMBER, RUNNUMBER)
        )
    """
    q_table2="""create table HLT_TEST_TRIGGERPATHS
        (
            RUNNUMBER      NUMBER(11)  NOT NULL, 
            LSNUMBER       NUMBER(11)  NOT NULL,
            PATHID         NUMBER(11)  NOT NULL,
            L1PASS         NUMBER(20)  NOT NULL,
            PSPASS         NUMBER(20)  NOT NULL,
            PACCEPT        NUMBER(20)  NOT NULL,
            PEXCEPT        NUMBER(20)  NOT NULL,
            PREJECT        NUMBER(20)  NOT NULL,
            primary key (LSNUMBER, RUNNUMBER, PATHID),
            foreign key (RUNNUMBER, LSNUMBER)    references
            HLT_TEST_LUMISECTIONS_V3 (RUNNUMBER, LSNUMBER)
        )
    """
    #q_table3="""create table HLT_TEST_L1_SCALARS
    #    (
    #        RUNNUMBER                                 NUMBER                                NOT NULL, 
    #        LSNUMBER                                  NUMBER                                NOT NULL,
    #        MODIFICATIONTIME                          TIMESTAMP(6)                          NOT NULL,
    #        DECISION_ARRAY                            CMS_DAQ_TEST_RUNINFO.L1_DECISION_VARRAY        NOT NULL, 
    #        DECISION_ARRAY_PHYSICS                    CMS_DAQ_TEST_RUNINFO.L1_DECISION_VARRAY, 
    #        DECISION_ARRAY_CALIBRATION                CMS_DAQ_TEST_RUNINFO.L1_DECISION_VARRAY, 
    #        DECISION_ARRAY_RANDOM                     CMS_DAQ_TEST_RUNINFO.L1_DECISION_VARRAY, 
    #        TECHNICAL_ARRAY                           CMS_DAQ_TEST_RUNINFO.L1_TECHNICAL_VARRAY       NOT NULL,
    #        TECHNICAL_ARRAY_PHYSICS                   CMS_DAQ_TEST_RUNINFO.L1_TECHNICAL_VARRAY,
    #        TECHNICAL_ARRAY_CALIBRATION               CMS_DAQ_TEST_RUNINFO.L1_TECHNICAL_VARRAY,
    #        TECHNICAL_ARRAY_RANDOM                    CMS_DAQ_TEST_RUNINFO.L1_TECHNICAL_VARRAY,
    #        EVENTCOUNT                                NUMBER(11),
    #        ENTRIESRCV                                NUMBER(11)
    #    )
    #"""
    q_table3="""create table HLT_TEST_SCALER_NAMES
        (
            TYPE                NUMBER(2),
            NAME                VARCHAR2(256),
            PRIMARY KEY (TYPE),
            CONSTRAINT UNIQUE_SCALER_NAME UNIQUE(NAME)
        )
    """
    databaseAgent.runQuery('hlt_rates_write', q_table3, False)
    databaseAgent.cxn_db['hlt_rates_write'].commit()
    databaseAgent.runQuery('hlt_rates_write', "INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 0, 'ALGORITHM_RATE_AFTER_PRESCALE'                            )", False)
    databaseAgent.runQuery('hlt_rates_write', "INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 1, 'ALGORITHM_RATE_BEFORE_PRESCALE'                           )", False)
    databaseAgent.runQuery('hlt_rates_write', "INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 2, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE'              )", False)
    databaseAgent.runQuery('hlt_rates_write', "INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 3, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_BY_HLT'       )", False)
    databaseAgent.runQuery('hlt_rates_write', "INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 4, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_PHYSICS'      )", False)
    databaseAgent.runQuery('hlt_rates_write', "INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 5, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_CALIBRATION'  )", False)
    databaseAgent.runQuery('hlt_rates_write', "INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 6, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_RANDOM'       )", False)
    databaseAgent.cxn_db['hlt_rates_write'].commit()
    q_table4="""create table HLT_TEST_LUMI_SECTIONS
        (
            ID                  VARCHAR2(13),
            RUN_NUMBER          NUMBER,
            LUMI_SECTION        NUMBER,
            PRESCALE_INDEX      NUMBER,
            PRIMARY KEY (ID),
            CONSTRAINT UNIQUE_LS UNIQUE(RUN_NUMBER, LUMI_SECTION)
        )
    """
    q_table5="""create table HLT_TEST_ALGO_SCALERS
        (
            ALGO_INDEX          NUMBER,
            ALGO_COUNT          NUMBER,
            ALGO_RATE           NUMBER(4),
            LUMI_SECTIONS_ID    VARCHAR2(13),
            SCALER_TYPE         NUMBER(2),
            PRIMARY KEY(ALGO_INDEX, LUMI_SECTIONS_ID, SCALER_TYPE),
            FOREIGN KEY (SCALER_TYPE) REFERENCES HLT_TEST_SCALER_NAMES( TYPE)
        )
    """
            #FOREIGN KEY (LUMI_SECTIONS_ID) REFERENCES HLT_TEST_LUMI_SECTIONS (ID),
    q_table6="""create table HLT_TEST_DATASETS
        (
            RUNNUMBER  NUMBER(11) NOT NULL,
            LSNUMBER   NUMBER(11) NOT NULL,
            DATASETID  NUMBER(11) NOT NULL,
            ACCEPT     NUMBER(20) NOT NULL
        )
    """
    q_table7="""create table HLT_TEST_STREAMS
        (
            RUNNUMBER NUMBER(11) NOT NULL,
            LSNUMBER  NUMBER(11) NOT NULL,
            STREAMID  NUMBER(11) NOT NULL,
            ACCEPT    NUMBER(20) NOT NULL
        )
    """    
    databaseAgent.runQuery('hlt_rates_write', q_table1, False)
    databaseAgent.runQuery('hlt_rates_write', q_table2, False)
    databaseAgent.runQuery('hlt_rates_write', q_table4, False)
    databaseAgent.runQuery('hlt_rates_write', q_table5, False)
    databaseAgent.runQuery('hlt_rates_write', q_table6, False)
    databaseAgent.runQuery('hlt_rates_write', q_table7, False)
    databaseAgent.cxn_db['hlt_rates_write'].commit()

# Don't run this in the production environment, ever.
def dropTestTables():
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_TRIGGERPATHS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_TRIGGERPATHS'; end if; end;", False)
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_LUMISECTIONS_V3'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_LUMISECTIONS_V3'; end if; end;", False)
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_DATASETS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_DATASETS'; end if; end;", False)
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_STREAMS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_STREAMS'; end if; end;", False)
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_ALGO_SCALERS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_ALGO_SCALERS'; end if; end;", False)
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_SCALER_NAMES'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_SCALER_NAMES'; end if; end;", False)
    databaseAgent.runQuery('hlt_rates_write', "declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_LUMI_SECTIONS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_LUMI_SECTIONS'; end if; end;", False)
    databaseAgent.cxn_db['hlt_rates_write'].commit()
