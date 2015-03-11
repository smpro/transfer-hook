#!/bin/env python

# Last modified by Dylan G. Hsu on 2014-12-11 :: dylan.hsu@cern.ch

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json
import logging

logger = logging.getLogger(__name__)
# Load Config file
#execfile('.db_integration_config.py')
execfile('/opt/transfers/.db_production_config.py')

# Supply this method with a FULL PATH to a .jsndata file to read it and put the HLT or L1 rates inside into the database.
# The jsndata needs the .ini descriptor files to be there or this will fail
#
# For the HLT rates, each lumisection has several path IDs which get their own row for that LS.
# We get these path IDs by looking up the path names provided in the .ini file in a mapping
# from the production database.
# For the L1 rates, we insert a single row per LS.
# However in this row are two Varrays containing 128 and 64 bits respectively, for the trigger rates, accomplishing in
# 1 row what the HLT rates table does with many rows.

def monitorRates(jsndata_file):
	# This takes the full path of a .jsndata file as parameter
	# Any other call of this function is inappropriate and will just not work!
	# e.g. jsndata_file='/store/lustre/mergeMiniDAQMacro/run230852/run230852_ls0110_streamHLTRates_mrg-c2f13-37-01.jsndata'

	json_dir=os.path.dirname(jsndata_file)
	jsndata_filename=os.path.basename(jsndata_file)
	file_raw, file_ext = os.path.splitext(jsndata_filename)
	raw_pieces=file_raw.split( '_' , 3 ) # this is not an emoji!
	run_number=raw_pieces[0][3:] # 123456
	ls=raw_pieces[1] # ls1234
	stream=raw_pieces[2][6:] # HLTRates | L1Rates
	extra=raw_pieces[3]

	if stream != "HLTRates" and stream != "L1Rates":
		logger.error('Unrecognized rate stream: '+raw_pieces[2])
		return False
	
	# Establish DB connections
	try:
		cxn_db_to_write=cx_Oracle.connect(write_db_login,write_db_pwd,write_db_sid)
	except cx_Oracle.DatabaseError as e:
		error, = e.args
		if error.code == 1017:
			logger.error('Bad credentials for database for writing rates')
			return False
		else:
			logger.error('Error connecting to database for writing: %s'.format(e))
			return False
	write_cursor=cxn_db_to_write.cursor()

	# We only need the trigger tables for the HLT rates:
	if stream=="HLTRates":
		try:
			cxn_db_to_read=cx_Oracle.connect(read_db_login,read_db_pwd,read_db_sid)
		except cx_Oracle.DatabaseError as e:
			if error.code == 1017:
				logger.error('Bad credentials for database for reading trigger tables')
				return False
			else:
				logger.error('Error connecting to database for reading: %s'.format(e))
				return False

		# Retrieve the mapping between HLT path index, HLT path name, HLT path ID
		pathname_query="""
			select B.SEQUENCENB, A.NAME, A.PATHID 
				from CMS_HLT.PATHS A, CMS_HLT.CONFIGURATIONPATHASSOC B, CMS_WBM.RUNSUMMARY C
				where
					A.PATHID = B.PATHID
					and B.CONFIGID = C.HLTKEY
					and C.RUNNUMBER = {0}
			order by B.SEQUENCENB
		"""
		read_cursor=cxn_db_to_read.cursor()
		read_cursor.execute(pathname_query.format(str(run_number)))
		pathname_mapping=read_cursor.fetchall()
		if len(pathname_mapping) < 1:
			logger.error("Could not get pathname-pathID mapping for HLT rates!")
			return False

	# Open the jsndata file
	# If it doesn't exist, this function will crash

	try:
		rates_json=open(jsndata_file).read()
	except (OSError, IOError) as e:
		logger.error('Error finding or opening jsndata file: "'+jsndata_file+'"')
		return False
	rates=json.loads(rates_json)

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
			HLT_rates[pathname]['L1PASS'] 	= rates['data'][2][i]
			HLT_rates[pathname]['PSPASS'] 	= rates['data'][3][i]
			HLT_rates[pathname]['PACCEPT'] 	= rates['data'][4][i]
			HLT_rates[pathname]['PREJECT'] 	= rates['data'][5][i]
			HLT_rates[pathname]['PEXCEPT'] 	= rates['data'][6][i]
			if HLT_LS_info['PROC']==0:
				HLT_LS_info['PROC']=rates['data'][0][0]
			i+=1

		# Before we put the rates in the DB, we will need see if the LS is indexed in the DB
		# If it isn't, we create a row in the table of LS
		# Currently most of the fields are set to 0 because I am grossly misinformed
		query="SELECT RUNNUMBER FROM "+HLT_LS_db+" WHERE LSNUMBER="+ls[2:]+" AND RUNNUMBER="+run_number
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
				HLT_LS_db,
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
			# print "Successfully inserted that LS"
				
		# Put the rates in the DB
		# This is kept separate from the above part because we might want to do something smarter with the path mapping later
		for pathname in HLT_rates:
			path_id=-1
			# Loop over tuples in the mapping to find the path id for this particular path name
			# This is klugey, but it's less memory intensive than using a dict
			for tuple in pathname_mapping:
				if pathname==tuple[1]:
					path_id=tuple[2]

			if path_id == -1:
				'Pathname "'+pathname+'" missing from pathname-pathID mapping!'
				return False

			query="""
				INSERT INTO {0} (
					RUNNUMBER, LSNUMBER, PATHID, L1PASS, PSPASS, PACCEPT, PEXCEPT, PREJECT
				) VALUES (
					{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}
				)
			"""
			query=query.format(
				HLT_rates_db,
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
		L1_rates['EVENTCOUNT'] 		= rates['data'][0][0]
		L1_rates['L1_DECISION'] 	= rates['data'][1]
		L1_rates['L1_TECHNICAL'] 	= rates['data'][2]
		L1_rates['mod_datetime']	= str(datetime.datetime.utcfromtimestamp(os.path.getmtime(jsndata_file)))
		# Here we record the file modification time of the jsndata file for book keeping purposes
		
		# Insert L1 rates into the database
		query="""
			INSERT INTO {0} (
				RUNNUMBER, LSNUMBER, MODIFICATIONTIME, DECISION_ARRAY, TECHNICAL_ARRAY, EVENTCOUNT
			) VALUES (
				{1}, {2}, {3}, {4}, {5}, {6}
			)
		"""
		query=query.format(
			L1_db,
			run_number,
			int(ls[2:]),
			"TO_TIMESTAMP('"+L1_rates['mod_datetime']+"','YYYY-MM-DD HH24:MI:SS.FF6')",
			decision_varray_name+'('+','.join(map(str,L1_rates['L1_DECISION']))+')', # VARRAY(1,2,3,4,...N)
			technical_varray_name+'('+','.join(map(str,L1_rates['L1_TECHNICAL']))+')', # VARRAY(1,2,3,4,...N)
			L1_rates['EVENTCOUNT']
		)
		write_cursor.execute(query)
		cxn_db_to_write.commit()
		return True

def makeWriteCxn():
	return cx_Oracle.connect(write_db_login,write_db_pwd,write_db_sid)
def makeReadCxn():
	return cx_Oracle.connect(read_db_login,read_db_pwd,read_db_sid)
def makeWriteCursor():
	cxn_db_to_write=cx_Oracle.connect(write_db_login,write_db_pwd,write_db_sid)
	return cxn_db_to_write.cursor()
def makeReadCursor():
	cxn_db_to_read=cx_Oracle.connect(read_db_login,read_db_pwd,read_db_sid)
	return cxn_db_to_read.cursor()

def getMyTables():
	cursor=makeWriteCxn().cursor()
	query = "SELECT owner, table_name FROM all_tables WHERE owner='CMS_DAQ_TEST_RUNINFO'"
	cursor.execute(query)
	for res in cursor:
		print res

def makeTestTables():
	cursor=makeWriteCxn().cursor()
	q_varray1="create or replace type L1_DECISION_VARRAY is VARRAY(128) of NUMBER(11)"
	q_varray2="create or replace type L1_TECHNICAL_VARRAY is VARRAY(64) of NUMBER(11)"
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
	q_table3="""create table HLT_TEST_L1_SCALARS
		(
			RUNNUMBER                                 NUMBER								NOT NULL, 
			LSNUMBER                                  NUMBER                                NOT NULL,
			MODIFICATIONTIME                          TIMESTAMP(6)                          NOT NULL,
			DECISION_ARRAY                            CMS_DAQ_TEST_RUNINFO.L1_DECISION_VARRAY        NOT NULL, 
			TECHNICAL_ARRAY                           CMS_DAQ_TEST_RUNINFO.L1_TECHNICAL_VARRAY       NOT NULL,
			EVENTCOUNT                                NUMBER(11),
			ENTRIESRCV                                NUMBER(11)
		)
	"""
	cursor.execute(q_varray1)
	cursor.execute(q_varray2);
	cursor.execute(q_table1)
	cursor.execute(q_table2);
	cursor.execute(q_table3);

def dropTestTables():
	cursor=makeWriteCxn().cursor()
	cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_TRIGGERPATHS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_TRIGGERPATHS'; end if; end;")
	cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_LUMISECTIONS_V3'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_LUMISECTIONS_V3'; end if; end;")
	cursor.execute("declare existing_tables number; begin select count(*) into existing_tables from all_tables where table_name = 'HLT_TEST_L1_SCALARS'; if existing_tables > 0 then execute immediate 'drop table HLT_TEST_L1_SCALARS'; end if; end;")

def outputTestTables():
	cursor=makeWriteCxn().cursor()
	print "######################################################################################################################"
	print HLT_LS_db
	cursor.execute('select * from '+HLT_LS_db)
	print cursor.fetchall()
	print "######################################################################################################################"
	print HLT_rates_db
	cursor.execute('select * from '+HLT_rates_db)
	print cursor.fetchall()
	print "######################################################################################################################"
	print L1_db
	cursor.execute("select * from "+L1_db)
	print cursor.fetchall()
	print "######################################################################################################################"
