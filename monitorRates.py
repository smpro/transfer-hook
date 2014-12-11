#!/bin/env python

# Last modified by Dylan G. Hsu on 2014-12-08

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json

# Loag Config file
execfile('.db_integration_config.py')
#execfile('.db_production_config.py')

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
		print 'Unrecognized rate stream: '+raw_pieces[2]
		return False
	
	# Establish DB connections
	try:
		cxn_db_to_write=cx_Oracle.connect(write_db_login,write_db_pwd,write_db_sid)
	except cx_Oracle.DatabaseError as e:
		error, = e.args
		if error.code == 1017:
			print 'Bad credentials for database for writing rates'
			return False
		else:
			print('Error connecting to database for writing: %s'.format(e))
			return False
	write_cursor=cxn_db_to_write.cursor()

	# We only need the trigger tables for the HLT rates:
	if stream=="HLTRates":
		try:
			cxn_db_to_read=cx_Oracle.connect(read_db_login,read_db_pwd,read_db_sid)
		except cx_Oracle.DatabaseError as e:
			if error.code == 1017:
				print 'Bad credentials for database for reading trigger tables'
				return False
			else:
				print('Error connecting to database for reading: %s'.format(e))
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
			print "Could not get pathname-pathID mapping for HLT rates!"
			return False

	# Open the jsndata file
	# If it doesn't exist, this function will crash

	try:
		rates_json=open(jsndata_file).read()
	except (OSError, IOError) as e:
		print 'Error finding or opening jsndata file: "'+jsndata_file+'"'
		return False
	rates=json.loads(rates_json)

	# Get the ini in the directory with rates corresponding to specified run number, lumi section, and HLT|L1
	# The correspondence might look like:
	# run230852_ls0110_streamL1Rates_mrg-c2f13-37-01.jsndata =>
	# run230852_ls0000_streamL1Rates_mrg-c2f13-37-01.ini
	# If the INI file is not there, this function will crash

	ini_filename=raw_pieces[0]+'_ls0000_'+raw_pieces[2]+'_'+raw_pieces[3]+'.ini'
	if stream=='HLTRates':
		try:
			HLT_json=open(json_dir+'/'+ini_filename).read()
		except (OSError, IOError) as e:
			print 'Error finding or opening ini file: "'+json_dir+'/'+ini_filename+'"'
		HLT_names=json.loads(HLT_json)
		HLT_rates={}
		i=0
		# Get the rates for each trigger path
		for pathname in HLT_names['Path-Names']:
			HLT_rates[pathname]={}
			HLT_rates[pathname]['L1PASS'] 	= rates['data'][2][i]
			HLT_rates[pathname]['PSPASS'] 	= rates['data'][3][i]
			HLT_rates[pathname]['PACCEPT'] 	= rates['data'][4][i]
			HLT_rates[pathname]['PREJECT'] 	= rates['data'][5][i]
			HLT_rates[pathname]['PEXCEPT'] 	= rates['data'][6][i]
			i+=1

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
				HLT_db,
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
			L1_json=open(json_dir+'/'+ini_filename).read()
		except (OSError, IOError) as e:
			print 'Error finding or opening ini file: "'+json_dir+'/'+ini_filename+'"'
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
	q_table1="""create table HLT_TEST_TRIGGERPATHS
		(
			RUNNUMBER      NUMBER(11)  NOT NULL, 
			LSNUMBER       NUMBER(11)  NOT NULL,
			PATHID         NUMBER(11)  NOT NULL,
			L1PASS         NUMBER(20)  NOT NULL,
			PSPASS         NUMBER(20)  NOT NULL,
			PACCEPT        NUMBER(20)  NOT NULL,
			PEXCEPT        NUMBER(20)  NOT NULL,
			PREJECT        NUMBER(20)  NOT NULL
		)
	"""
	q_table2="""create table HLT_TEST_L1_SCALARS
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

def dropTestTables():
	cursor=makeWriteCxn().cursor()
	cursor.execute("drop table HLT_TEST_TRIGGERPATHS")
	cursor.execute("drop table HLT_TEST_L1_SCALARS")

def outputTestTables():
	cursor=makeWriteCxn().cursor()
	cursor.execute('select * from '+HLT_db)
	print cursor.fetchall()
	cursor.execute("select * from "+L1_db)
	print cursor.fetchall()
