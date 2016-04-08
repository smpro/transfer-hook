#!/bin/env python

# Last modified by Dylan G. Hsu on 2016-03-07 :: dylan.hsu@cern.ch

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json
import logging

import smhook.config

# Hardcoded Config file to be used, is defined below:
myconfig = os.path.join(smhook.config.DIR, '.db_rcms_cred.py')
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
# fileQualityControl        #
#############################
#
# Supply this method with a FULL PATH to a .jsndata file to read it and put the HLT or L1 rates inside into the database.
# 		As of May 2015 we also put the dataset acceptances in another table for the HLT
#		and store the L1 rates type by type.

# The jsndata needs the .ini descriptor files to exist in relative subdirectory "./open" or this will fail!
#
# For the HLT rates, each lumisection has several path IDs which get their own row for that LS.
# We get these path IDs by looking up the path names provided in the .ini file in a mapping
# from the production database.
# For the L1 rates, we insert a single row per LS, per type (algo|technical)x(all|physics|calibration|random)
# However in this row are two Varrays containing 128 (64) bits for the algo (technical) for the trigger rates,
# accomplishing in 1 row what the HLT rates table does with many rows.

def fileQualityControl(filename, events_built, events_lost, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls):
	# This inserts the information in the database
	jsn_file = rates_jsn_file
	json_dir=os.path.dirname(jsndata_file) 
	jsndata_filename=os.path.basename(jsndata_file)
	file_raw, file_ext = os.path.splitext(jsndata_filename)
	raw_pieces=file_raw.split( '_' , 3 ) # this is not an emoji!
	run_number=raw_pieces[0][3:] # 123456
	ls=raw_pieces[1] # ls1234
	stream=raw_pieces[2][6:] # HLTRates | L1Rates
	machine=raw_pieces[3]
	
	#if stream != "HLTRates" and stream != "L1Rates":
	#	logger.error('Unrecognized rate stream: '+raw_pieces[2])
	#	return False
	
	# Establish DB connections
	try:
		cxn_db=cx_Oracle.connect(db_user, db_pwd, db_sid)
	except cx_Oracle.DatabaseError as e:
		error, = e.args
		if error.code == 1017:
			logger.error('Bad credentials for database for writing file quality control')
			return False
		else:
			logger.error('Error connecting to database for writing: %s'.format(e))
			return False
	cursor=cxn_db.cursor()
	query="SELECT FILENAME FROM CMS_STOMGR.FILE_QUALITY_CONTROL WHERE FILENAME='"+filename+"'"
	cursor.execute(query)
    if(is_good_ls):
		is_good_ls=1
	else:
		is_good_ls=0
	if len(cursor.fetchall()) < 1:
		# No existing row. we must now try to insert:
		query="""
			INSERT INTO CMS_STOMGR.FILE_QUALITY_CONTROL (
				RUNNUMBER,
				LS,
				STREAM,
				FILENAME,
				LAST_UPDATE_TIME,
				EVENTS_BUILT,
				EVENTS_LOST,
				EVENTS_LOST_CHECKSUM,
				EVENTS_LOST_CMSSW,
				EVENTS_LOST_CRASH,
				EVENTS_LOST_OVERSIZED,
				IS_GOOD_LS
			) VALUES (
				{1}, {2}, '{3}', '{4}', {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}
			)
		"""
		query=query.format(
			"CMS_STOMGR.FILE_QUALITY_CONTROL",
			run_number,
			ls[2:],
			stream,
			filename,
			"TO_TIMESTAMP('"+str(datetime.datetime.utcfromtimestamp(os.path.getmtime(jsndata_file)))+"','YYYY-MM-DD HH24:MI:SS.FF6')", #UTC timestamp -> oracle
			events_built,
			events_lost,
			events_lost_checksum,
			events_lost_cmssw,
			events_lost_crash,
			events_lost_oversized,
			is_good_ls
		)
		cursor.execute(query)
		cxn_db.commit()
	else:
		query="""
			UPDATE CMS_STOMGR.FILE_QUALITY_CONTROL SET
				RUNNUMBER              = {1},
				LS                     = {2},
				STREAM                 = '{3}',
				FILENAME               = '{4}',
				LAST_UPDATE_TIME       = {5},
				EVENTS_BUILT           = {6},
				EVENTS_LOST            = {7},
				EVENTS_LOST_CHECKSUM   = {8},
				EVENTS_LOST_CMSSW      = {9},
				EVENTS_LOST_CRASH      = {10},
				EVENTS_LOST_OVERSIZED  = {11},
				IS_GOOD_LS             = {12}
			WHERE FILENAME='{4}'
		"""
		query=query.format(
			"CMS_STOMGR.FILE_QUALITY_CONTROL",
			run_number,
			ls[2:],
			stream,
			filename,
			"TO_TIMESTAMP('"+str(datetime.datetime.utcfromtimestamp(os.path.getmtime(jsndata_file)))+"','YYYY-MM-DD HH24:MI:SS.FF6')", #UTC timestamp -> oracle
			events_built,
			events_lost,
			events_lost_checksum,
			events_lost_cmssw,
			events_lost_crash,
			events_lost_oversized,
			is_good_ls
		)
		cursor.execute(query)
		cxn_db.commit()
		return True
