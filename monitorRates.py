#!/bin/env python

# Last modified by Dylan G. Hsu on 2014-12-05

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json

# Loag Config file
execfile('.db_integration_config.py')
#execfile('.db_production_config.py')

# json_dir='/nfshome0/hltpro/jsonMonTest/exampleJsons/fixedPseudoStream'

# Supply this method with a directory to find the json files and a run number.
# It will loop over these files to find the .ini files then take a look at the .jsndata files.
# This information will be summed in two dicts across PIDs, where the dicts are indexed by lumisection.
# If PIDs are summed over, then the sum doesn't matter and we just get 1 file.
# Afterwards, the two dicts are inserted into the DB.
#
# For the HLT rates, each lumisection has several path IDs which get their own row for that LS.
# We get these path IDs by looking up the path names provided in the .ini file in a mapping
# from the production database.
# For the L1 rates, we insert a single row per LS.
# However in this row are two Varrays containing 128 and 64 bits respectively, for the trigger rates, accomplishing in
# 1 row what the HLT rates table does with many rows.


def monitorRates(json_dir,run_number):
	cxn_db_to_write=cx_Oracle.connect(write_db_login,write_db_pwd,write_db_sid)
	cxn_db_to_read=cx_Oracle.connect(read_db_login,read_db_pwd,read_db_sid)
	# Get all ini files in the directory with rates corresponding to specified run number
	filenames=[f for f in os.listdir(json_dir) if f.startswith('run'+str(run_number)) and f.endswith('.ini')]
	for filename in filenames:
		file_raw, file_ext = os.path.splitext(filename)
		raw_pieces=file_raw.split( '_' ) # this is not an emoji!
		ls=raw_pieces[1]
		stream=raw_pieces[2][6:]
		#pid=raw_pieces[3][3:]
		# Put the name indices into variables
		if stream=='HLTRates':
			HLT_names=json.loads(open(json_dir+'/'+filename).read())
		elif stream=='L1Rates':
			L1_names=json.loads(open(json_dir+'/'+filename).read())

	# Define dicts for the HLT rates and L1 rates
	HLT_rates={}
	L1_rates={}
	
	# Get all jsndata files in the directory with rates corresponding to specified run number
	filenames=[f for f in os.listdir(json_dir) if f.startswith('run'+str(run_number)) and f.endswith('.jsndata')]
	for filename in filenames:
		file_raw, file_ext = os.path.splitext(filename)
		raw_pieces=file_raw.split( '_' ) # this is not an emoji!
		ls=raw_pieces[1]
		stream=raw_pieces[2][6:]
		#pid=raw_pieces[3][3:]

		rates=json.loads(open(json_dir+'/'+filename).read())

		if stream=='HLTRates':
			# We made dict for HLT rates which will contain one key for each lumi section
			# If the key isn't created, we make it and put in the rates for the particular pid of that file
			# If it already exists, we add the rates for this pid to the other ones
			# This sums over all PIDs for the given lumi section
			if ls not in HLT_rates:
				HLT_rates[ls]={}
				i=0
				for pathname in HLT_names['Path-Names']:
					HLT_rates[ls][pathname]={}
					HLT_rates[ls][pathname]['L1PASS'] 	= rates['data'][2][i]
					HLT_rates[ls][pathname]['PSPASS'] 	= rates['data'][3][i]
					HLT_rates[ls][pathname]['PACCEPT'] 	= rates['data'][4][i]
					HLT_rates[ls][pathname]['PREJECT'] 	= rates['data'][5][i]
					HLT_rates[ls][pathname]['PEXCEPT'] 	= rates['data'][6][i]
					i+=1
			else:
				i=0
				for pathname in HLT_names['Path-Names']:
					HLT_rates[ls][pathname]['L1PASS'] 	+= rates['data'][2][i]
					HLT_rates[ls][pathname]['PSPASS'] 	+= rates['data'][3][i]
					HLT_rates[ls][pathname]['PACCEPT'] 	+= rates['data'][4][i]
					HLT_rates[ls][pathname]['PREJECT'] 	+= rates['data'][5][i]
					HLT_rates[ls][pathname]['PEXCEPT'] 	+= rates['data'][6][i]
					i+=1

		elif stream=='L1Rates':
			# We now follow similar method for L1 rates as done for HLT rates above
			# The data is different
			if ls not in L1_rates:
				L1_rates[ls]={}
				L1_rates[ls]['EVENTCOUNT'] 		= rates['data'][0][0]
				L1_rates[ls]['L1_DECISION'] 	= rates['data'][1]
				L1_rates[ls]['L1_TECHNICAL'] 	= rates['data'][2]
				L1_rates[ls]['mod_datetime']	= str(datetime.datetime.fromtimestamp(os.path.getmtime(json_dir+'/'+filename)))
				# I only record the file modification time of the first L1 jsndata file discovered for this lumi section
				# This is probably an issue but nobody knows the answer
			else:
				# new total += the increment
				L1_rates[ls]['EVENTCOUNT'] 		+= rates['data'][0][0]
				L1_rates[ls]['L1_DECISION'] 	= [ x+y for x,y in zip(L1_rates[ls]['L1_DECISION'],rates['data'][1]) ]
				L1_rates[ls]['L1_TECHNICAL'] 	= [ x+y for x,y in zip(L1_rates[ls]['L1_TECHNICAL'],rates['data'][2]) ]

		#print file_raw+' is a '+file_ext+' file'i

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
	write_cursor=cxn_db_to_write.cursor()

	read_cursor.execute(pathname_query.format(str(run_number)))
	pathname_mapping=read_cursor.fetchall()
	if len(pathname_mapping) < 1:
		print "Could not get pathname-pathID mapping!"
		return false
	for ls in HLT_rates:

		for pathname in HLT_rates[ls]:
			path_id=-1
			# Loop over tuples in the mapping to find the path id for this particular path name
			# This is klugey but it's less memory intensive than using a dict
			for tuple in pathname_mapping:
				if pathname==tuple[1]:
					path_id=tuple[2]

			if path_id == -1:
				'Pathname "'+pathname+'" missing from pathname-pathID mapping!'
				return false

			query="""
				INSERT INTO CMS_DAQ_TEST_RUNINFO.HLT_TEST_TRIGGERPATHS (
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
				HLT_rates[ls][pathname]['L1PASS'], 
            	HLT_rates[ls][pathname]['PSPASS'],
            	HLT_rates[ls][pathname]['PACCEPT'],
            	HLT_rates[ls][pathname]['PEXCEPT'],
            	HLT_rates[ls][pathname]['PREJECT']
			)
			#print query
			write_cursor.execute(query)
			cxn_db_to_write.commit()

	for ls in L1_rates:
		
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
			"TO_TIMESTAMP('"+L1_rates[ls]['mod_datetime']+"','YYYY-MM-DD HH24:MI:SS.FF6')",
			decision_varray_name+'('+','.join(map(str,L1_rates[ls]['L1_DECISION']))+')', # VARRAY(1,2,3,4,...N)
			technical_varray_name+'('+','.join(map(str,L1_rates[ls]['L1_TECHNICAL']))+')', # VARRAY(1,2,3,4,...N)
			L1_rates[ls]['EVENTCOUNT']
		)
		#print query
		write_cursor.execute(query)
		cxn_db_to_write.commit()
		
		
	#write_cursor.execute('SELECT * FROM '+HLT_db)
	#print 'fetching all from TRIGGERPATHS:'
	#print write_cursor.fetchall()
	#write_cursor.execute('SELECT * FROM '+L1_db)
	#print 'fetching all from L1:'
	#print write_cursor.fetchall()
	#write_cursor.execute('SELECT COUNT(*) FROM '+HLT_db)
	#print 'fetching count from TRIGGERPATHS:'
	#print write_cursor.fetchall()
	#
	#outputTestTables()
	
	
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
	cursor=makeWriteCxn.cursor()
	query = "SELECT owner, table_name FROM all_tables WHERE owner='CMS_DAQ_TEST_RUNINFO'"
	cursor.execute(query)
	for res in cursor:
		print res

def makeTestTables():
	cursor=makeWriteCxn.cursor()
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
	cursor=makeWriteCxn.cursor()
	cursor.execute("drop table HLT_TEST_TRIGGERPATHS")
	cursor.execute("drop table HLT_TEST_L1_SCALARS")

def outputTestTables():
	cursor=makeWriteCxn().cursor()
	cursor.execute('select * from '+HLT_db)
	print cursor.fetchall()
	cursor.execute("select * from "+L1_db)
	print cursor.fetchall()
