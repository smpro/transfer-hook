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
# This function takes a full path to a json file, the filename of a data file, and several numeric arguments then inserts or updates the relevant information in the database
# The number of events built is greater than or equal to number of events lost.
# The database configuration is taken from smhook.config

def fileQualityControl(jsn_file, data_file, events_built, events_lost_checksum, events_lost_cmssw, events_lost_crash, events_lost_oversized, is_good_ls):
	events_lost = min(events_built, events_lost_checksum + events_lost_cmssw + events_lost_crash + events_lost_oversized)
	# This inserts the information in the database
	file_raw, file_ext = os.path.splitext(data_file)
	raw_pieces=file_raw.split( '_' , 3 ) # this is not an emoji! C-('_' Q)
	run_number=raw_pieces[0][3:] # 123456
	ls=raw_pieces[1] # ls1234
	stream=raw_pieces[2][6:] 
	machine=raw_pieces[3]
	
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
	query="SELECT FILENAME FROM CMS_STOMGR.FILE_QUALITY_CONTROL WHERE FILENAME='"+data_file+"'"
	# See if there is an existing row
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
			data_file,
			"TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", #UTC timestamp -> oracle
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
		# Update the existing row
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
			data_file,
			"TO_TIMESTAMP('"+str(datetime.datetime.utcnow())+"','YYYY-MM-DD HH24:MI:SS.FF6')", #UTC timestamp -> oracle
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
