#!/bin/env python

# Last modified by Dylan G. Hsu on 2015-05-29 :: dylan.hsu@cern.ch

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json
import logging

import smhook.config

# Hardcoded Config file to be used, is defined below:
# We read from production DB no matter what (in either case)
# but for testing, write to integration DB only

#myconfig = os.path.join(smhook.config.DIR, '.db_rates_integration.py')
myconfig = os.path.join(smhook.config.DIR, '.db_rates_production.py')
debug=False
logger = logging.getLogger(__name__)
# For debugging purposes, initialize the logger to stdout if running script as a standalone
if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

# Load the config
logger.info('Using config: %s' % myconfig)
execfile(myconfig)

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
        try:
            cxn_db_to_read=cx_Oracle.connect(trigger_db_login,trigger_db_pwd,trigger_db_sid)
        except cx_Oracle.DatabaseError as e:
            if error.code == 1017:
                logger.error('Bad credentials for database for reading trigger tables/datasets')
                return False
            else:
                logger.error('Error connecting to database for reading: %s'.format(e))
                return False

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
        """
        read_cursor=cxn_db_to_read.cursor()
        read_cursor.execute(pathname_query.format(str(run_number)))
        pathname_mapping=read_cursor.fetchall()
        if len(pathname_mapping) < 1:
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
        """
        read_cursor=cxn_db_to_read.cursor()
        read_cursor.execute(dataset_name_query.format(str(run_number)))
        dataset_name_mapping=read_cursor.fetchall()
        if len(dataset_name_mapping) < 1:
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
        # Establish DB connections for HLT
        try:
            cxn_db_to_write=cx_Oracle.connect(hlt_rates_db_login, hlt_rates_db_pwd, hlt_rates_db_sid)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                logger.error('Bad credentials for database for writing rates')
                return False
            else:
                logger.error('Error connecting to database for writing: %s'.format(e))
                return False
        write_cursor=cxn_db_to_write.cursor()

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
        write_cursor.execute(query)
        if len(write_cursor.fetchall()) < 1:
            # No existing row. we must now try to insert:
            # print "This LS is not already in the DB" #debug    
            query="""
                INSERT INTO {0} (
                    RUNNUMBER, LSNUMBER, MODIFICATIONTIME, PSINDEX, PSINDMATCH, PROC, ACC, ENTRIESRCV, ENTRIESEXP, EFFREP, EXPREP,  MODDIFF
                ) VALUES (
                    {1},       {2},      {3},              {4},     {5},        {6},  {7}, {8},        {9},        {10},   {11},    {12}
                )
            """
            query=query.format(
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
            write_cursor.execute(query)
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
            """
            query=query.format(
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
            write_cursor.execute(query)
            cxn_db_to_write.commit()

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
            """
            query=query.format(
                HLT_datasets_db,
                run_number,
                int(ls[2:]),
                dataset_id,
                HLT_dataset_acceptances[dataset_name]
            )
            write_cursor.execute(query)
            cxn_db_to_write.commit()
        return True
    
    elif stream=='L1Rates':
        # Establish DB connection for L1
        try:
            cxn_db_to_write=cx_Oracle.connect(l1_rates_db_login, l1_rates_db_pwd, l1_rates_db_sid)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                logger.error('Bad credentials for database for writing L1 rates')
                return False
            else:
                logger.error('Error connecting to database for writing: %s'.format(e))
                return False
        write_cursor=cxn_db_to_write.cursor()

        try:
            cxn_db_to_read=cx_Oracle.connect(l1_rate_type_db_login, l1_rate_type_db_pwd, l1_rate_type_db_sid)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                logger.error('Bad credentials for database for reading L1 rate types')
                return False
            else:
                logger.error('Error connecting to database for reading: %s'.format(e))
                return False
        read_cursor=cxn_db_to_read.cursor()

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
        
        # Check if the LS is already registered in the database
        # Currently not needed.
        #query="SELECT ID FROM {0} WHERE RUN_NUMBER={1} AND LUMI_SECTION={2}".format(L1_lumisection_id_table, run_number, int(ls[2:]))
        #write_cursor.execute(query);
        #result=write_cursor.fetchall()
        #if len(result) < 1 and False:
        #    # No existing row. we must now try to insert it in the L1 DB:
        #    lumisection_id = "%06d_%05d" % (int(run_number), int(ls[2:]))
        #    query="INSERT INTO %s (ID, RUN_NUMBER, LUMI_SECTION) VALUES ('%s', %d, %d)" % (
        #        L1_lumisection_id_table, 
        #        lumisection_id,
        #        int(run_number),
        #        int(ls[2:])
        #    )
        #    write_cursor.execute(query)
        #    cxn_db_to_write.commit()

        #else:
        #    lumisection_id = result[0][0]
        lumisection_id = "%06d_%05d" % (int(run_number), int(ls[2:]))

        # Retrieve the IDs for the different types of L1 rates from the lookup table
        try:
            read_cursor.execute("SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_BY_HLT') FROM DUAL")
            l1_all_rates_result = read_cursor.fetchall()
            read_cursor.execute("SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_PHYSICS') FROM DUAL")
            l1_physics_rates_result = read_cursor.fetchall()
            read_cursor.execute("SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_CALIBRATION') FROM DUAL")
            l1_calibration_rates_result = read_cursor.fetchall()
            read_cursor.execute("SELECT CMS_UGT_MON.GET_SCALER_TYPE('POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_RANDOM') FROM DUAL")
            l1_random_rates_result = read_cursor.fetchall()
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            logger.error('Error with database while looking up the ID for the L1 rate types: %s'.format(e))
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
                write_cursor.execute(query)
                algo_index=algo_index+1
        cxn_db_to_write.commit()
        cxn_db_to_write.close()
        return True

        # OLD CODE FOLLOWS
        ## Check if the L1 rates are split by type ( backwards compatibility )
        #L1_names=json.loads(L1_json)
        #L1_rates={}
        #L1_rates['EVENTCOUNT']                 = rates['data'][0][0]
        #L1_rates['L1_DECISION']             = rates['data'][1]
        #L1_rates['L1_TECHNICAL']             = rates['data'][2]
        #if len(rates['data'])>4:
        #    L1_rates['L1_DECISION_PHYSICS']        = rates['data'][3] # NEW LINES -DGH
        #    L1_rates['L1_TECHNICAL_PHYSICS']    = rates['data'][4]
        #    L1_rates['L1_DECISION_CALIBRATION']    = rates['data'][5]
        #    L1_rates['L1_TECHNICAL_CALIBRATION']= rates['data'][6]
        #    L1_rates['L1_DECISION_RANDOM']         = rates['data'][7]
        #    L1_rates['L1_TECHNICAL_RANDOM']     = rates['data'][8]
        ## Here we record the file modification time of the jsndata file for book keeping purposes
        #L1_rates['mod_datetime']            = str(datetime.datetime.utcfromtimestamp(os.path.getmtime(jsndata_file)))
        #
        ## Insert L1 rates into the database
        #if len(rates['data'])>4:
        #    query="""
        #        INSERT INTO {0} (
        #            RUNNUMBER,
        #            LSNUMBER,
        #            MODIFICATIONTIME,
        #            EVENTCOUNT,
        #            DECISION_ARRAY,
        #            DECISION_ARRAY_PHYSICS,
        #            DECISION_ARRAY_CALIBRATION,
        #            DECISION_ARRAY_RANDOM,
        #            TECHNICAL_ARRAY,
        #            TECHNICAL_ARRAY_PHYSICS,
        #            TECHNICAL_ARRAY_CALIBRATION,
        #            TECHNICAL_ARRAY_RANDOM
        #        ) VALUES (
        #            {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}
        #        )
        #    """
        #    # The join operations below simply format the rates arrays so that they may be inserted into the DB.
        #    query=query.format(
        #        L1_rates_db,
        #        run_number,
        #        int(ls[2:]),
        #        "TO_TIMESTAMP('"+L1_rates['mod_datetime']+"','YYYY-MM-DD HH24:MI:SS.FF6')",
        #        L1_rates['EVENTCOUNT'],
        #        decision_varray_name+'('+','.join(map(str,L1_rates['L1_DECISION']))+')', # VARRAY(1,2,3,4,...N)
        #        decision_varray_name+'('+','.join(map(str,L1_rates['L1_DECISION_PHYSICS']))+')',
        #        decision_varray_name+'('+','.join(map(str,L1_rates['L1_DECISION_CALIBRATION']))+')',
        #        decision_varray_name+'('+','.join(map(str,L1_rates['L1_DECISION_RANDOM']))+')',
        #        technical_varray_name+'('+','.join(map(str,L1_rates['L1_TECHNICAL']))+')', # VARRAY(1,2,3,4,...N)
        #        technical_varray_name+'('+','.join(map(str,L1_rates['L1_TECHNICAL_PHYSICS']))+')',
        #        technical_varray_name+'('+','.join(map(str,L1_rates['L1_TECHNICAL_CALIBRATION']))+')',
        #        technical_varray_name+'('+','.join(map(str,L1_rates['L1_TECHNICAL_RANDOM']))+')'
        #    )
        #else: # backwards compatible
        #    query="""
        #        INSERT INTO {0} (
        #            RUNNUMBER,
        #            LSNUMBER,
        #            MODIFICATIONTIME,
        #            EVENTCOUNT,
        #            DECISION_ARRAY,
        #            TECHNICAL_ARRAY
        #        ) VALUES (
        #            {1}, {2}, {3}, {4}, {5}, {6}
        #        )
        #    """
        #    # The join operations below simply format the rates arrays so that they may be inserted into the DB.
        #    query=query.format(
        #        L1_rates_db,
        #        run_number,
        #        int(ls[2:]),
        #        "TO_TIMESTAMP('"+L1_rates['mod_datetime']+"','YYYY-MM-DD HH24:MI:SS.FF6')",
        #        L1_rates['EVENTCOUNT'],
        #        decision_varray_name+'('+','.join(map(str,L1_rates['L1_DECISION']))+')', # VARRAY(1,2,3,4,...N)
        #        technical_varray_name+'('+','.join(map(str,L1_rates['L1_TECHNICAL']))+')' # VARRAY(1,2,3,4,...N)
        #    )        
        #write_cursor.execute(query)
        #cxn_db_to_write.commit()
        #return True

#def makeWriteCxn():
#    return cx_Oracle.connect(hlt_rates_db_login,write_db_pwd,write_db_sid)
#def makeReadCxn():
#    return cx_Oracle.connect(trigger_db_login,trigger_db_pwd,trigger_db_sid)
#def makeWriteCursor():
#    cxn_db_to_write=cx_Oracle.connect(hlt_rates_db_login,write_db_pwd,write_db_sid)
#    return cxn_db_to_write.cursor()
#def makeReadCursor():
#    cxn_db_to_read=cx_Oracle.connect(trigger_db_login,trigger_db_pwd,trigger_db_sid)
#    return cxn_db_to_read.cursor()

def getMyTables():
    cxn=cx_Oracle.connect(hlt_rates_db_login, hlt_rates_db_pwd, hlt_rates_db_sid)
    cursor=cxn.cursor()
    query = "SELECT owner, table_name FROM all_tables WHERE owner='CMS_DAQ_TEST_RUNINFO'"
    cursor.execute(query)
    for res in cursor:
        print res

# This method is used for testing only and should not be run in the production environment, ever!
# Also serves as internal documentation of the necessary tables because this stuff is not written down anywhere...
def makeTestTables():
    cxn=cx_Oracle.connect(hlt_rates_db_login, hlt_rates_db_pwd, hlt_rates_db_sid)
    cursor=cxn.cursor()
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
    cursor.execute(q_table3)
    cxn.commit()
    cursor.execute("INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 0, 'ALGORITHM_RATE_AFTER_PRESCALE'                            )")
    cursor.execute("INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 1, 'ALGORITHM_RATE_BEFORE_PRESCALE'                           )")
    cursor.execute("INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 2, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE'              )")
    cursor.execute("INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 3, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_BY_HLT'       )")
    cursor.execute("INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 4, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_PHYSICS'      )")
    cursor.execute("INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 5, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_CALIBRATION'  )")
    cursor.execute("INSERT INTO HLT_TEST_SCALER_NAMES(TYPE, NAME) VALUES ( 6, 'POST_DEADTIME_ALGORITHM_RATE_AFTER_PRESCALE_RANDOM'       )")
    cxn.commit()
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
    #cursor.execute(q_varray1)
    #cursor.execute(q_varray2);
    cursor.execute(q_table1)
    cursor.execute(q_table2);
    #cursor.execute(q_table3);
    #cursor.execute(q_fill_table3);
    cursor.execute(q_table4);
    cursor.execute(q_table5);
    cursor.execute(q_table6);
    cursor.execute(q_table7);
    cxn.commit()


# Don't run this in the production environment, ever.
def dropTestTables():
    cxn=cx_Oracle.connect(hlt_rates_db_login, hlt_rates_db_pwd, hlt_rates_db_sid)
    cursor=cxn.cursor()
    cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_TRIGGERPATHS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_TRIGGERPATHS'; end if; end;")
    cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_LUMISECTIONS_V3'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_LUMISECTIONS_V3'; end if; end;")
    cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_DATASETS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_DATASETS'; end if; end;")
    cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_STREAMS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_STREAMS'; end if; end;")
    cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_ALGO_SCALERS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_ALGO_SCALERS'; end if; end;")
    cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_SCALER_NAMES'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_SCALER_NAMES'; end if; end;")
    cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_LUMI_SECTIONS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_LUMI_SECTIONS'; end if; end;")
    cxn.commit()

def outputTestTables():
    # needs to be updated
    cxn=cx_Oracle.connect(hlt_rates_db_login, hlt_rates_db_pwd, hlt_rates_db_sid)
    cursor=cxn.cursor()
    print "######################################################################################################################"
    print HLT_LS_table
    cursor.execute('select * from '+HLT_LS_table)
    print cursor.fetchall()
    print "######################################################################################################################"
    print HLT_rates_table
    cursor.execute('select * from '+HLT_rates_table)
    print cursor.fetchall()
    print "######################################################################################################################"
    #print HLT_datasets_table
    #cursor.execute('select * from '+HLT_datasets_table)
    #print cursor.fetchall()
    #print "######################################################################################################################"
    print L1_rates_table
    cursor.execute("select * from "+L1_rates_table)
    print cursor.fetchall()
    print "######################################################################################################################"
