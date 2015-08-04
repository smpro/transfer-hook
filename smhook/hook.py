#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
TODO:
   * Query the DB more efficiently similar to ~/smpro/scripts/checkRun.pl
'''

import cx_Oracle
import datetime
import errno
import glob 
import json
import logging
import os
import pprint
import shlex
import shutil
import socket
import subprocess
import sys
import time
import multiprocessing
import zlib
from multiprocessing.pool import ThreadPool

import smhook.bookkeeper as bookkeeper
import smhook.monitorRates as monitorRates
import smhook.metafile as metafile
import smhook.eor as eor
import smhook.config as config

from datetime import datetime, timedelta, date
from optparse import OptionParser
from subprocess import call

from smhook.runinfo import RunInfo
from smhook.config import Config

#from Logging import getLogger
logger = logging.getLogger(__name__)


#______________________________________________________________________________
def main():
	'''
	Main entry point to execution.
	'''
	setup()

	_input_path = cfg.get('Input', 'path')
	_max_exceptions = cfg.getint('Misc','max_exceptions')
	_seconds_to_sleep = cfg.getint('Misc','seconds_to_sleep')
	_max_iterations = cfg.getfloat('Misc', 'max_iterations')
	_setup_label=cfg.get('Input','setup_label')
	if _setup_label in ['Data','Minidaq','TransferTestWithSafety']:
		logger.info('Doing main data process. input path is {0}'.format(_input_path))
	if _setup_label in ['PrivateData']:
		logger.info('Doing private data process. input path is {0}'.format(_input_path))
	caught_exception_count = 0
	iteration = 0
	while True:
		iteration += 1
		if iteration > _max_iterations:
			break
		logger.info('Start iteration {0} of {1} ...'.format(iteration,
															_max_iterations))
		try:
			setup_label=cfg.get('Input','setup_label')
			if _setup_label in ['Data','Minidaq','TransferTestWithSafety']:
				iterate_main_data()
			if _setup_label in ['PrivateData']:
				iterate_private_data()
		except Exception as e:
			caught_exception_count += 1
			logger.info(
				'Caught untreated exception number {0}'.format(
					caught_exception_count
					)
			)
			logger.exception(e)
			if caught_exception_count < _max_exceptions:
				logger.info(
					'Will give up if I reach {0} exceptions.'.format(
						_max_exceptions
					)
				)
				logger.info('Trying to iterate again for now ...')
			else:
				logger.critical('Too many errors! Giving up ...')
				raise e
		logger.info('Sleeping {0} seconds ...'.format(_seconds_to_sleep))
		time.sleep(_seconds_to_sleep)

	if _setup_label in ['Data','Minidaq','TransferTestWithSafety']:
		logger.info('Closing ECAL, DQM, and Event Diplay thread pools.')
		ecal_pool.close()
		dqm_pool.close()
		evd_pool.close()
		logger.info('Joining ECAL, DQM, and Event Display thread pools.')
		ecal_pool.join()
		dqm_pool.join()
		evd_pool.join()
	if _setup_label in ['PrivateData']:
		logger.info('Closing PDT transfer thread pool.')
		pdt_pool.close()
		logger.info('Joining PDT transfer thread pool.')
		pdt_pool.join()
## main()


#______________________________________________________________________________
def parse_args():
	parser = OptionParser(usage="usage: %prog [-h|--help] [-p|--path]")
	parser.add_option("-p", "--path",
					  action="store", dest="path",
					  help="path to watch for files to be transferred")
	## Add an option to path were the files should be moved after being
	## injected in the transfer
	options, args = parser.parse_args()
	if len(args) != 0:
		parser.error('You specified an invalid option - please use -h to '
					 'review the allowed options')
	if (options.path == None):
		parser.error('Please provide the path to watch')
	return options, args
## parse_args()

#______________________________________________________________________________
def setup():
	global log_and_maybe_exec
	global maybe_move
	global runinfo
	global ecal_pool
	global dqm_pool
	global evd_pool
	global lookarea_pool
	global pdt_pool
	global cfg
	cfg = config.config
	logger.info(
		'Using config file(s): %s ...' % ', '.join(cfg.filenames)
	)

	_dry_run = cfg.getboolean('Misc','dry_run')

	bookkeeper._dry_run = _dry_run
	bookkeeper.setup()
	runinfo = RunInfo(os.path.join(config.DIR, '.db.omds.runinfo_r.cfg.py'))
	if _dry_run:
		log_and_maybe_exec = log_and_do_not_exec
		maybe_move = mock_move_file_to_dir
	else:
		log_and_maybe_exec = log_and_exec
		maybe_move = move_file_to_dir

	_setup_label=cfg.get('Input','setup_label')
	if _setup_label in ['Data','Minidaq','TransferTestWithSafety']:
		ecal_pool	 = ThreadPool(4)
		dqm_pool	  = ThreadPool(4)
		evd_pool	  = ThreadPool(4)
		lookarea_pool = ThreadPool(4)
	if _setup_label in ['PrivateData']:
		pdt_pool	  = ThreadPool(50)
## setup()

#______________________________________________________________________________
def iterate_main_data():
	
	path = cfg.get('Input', 'path')
	#_scratch_base = cfg.get('Output','scratch_base')
	#new_path_base = cfg.get('Output', 'new_path_base')
	new_path = cfg.get('Output','new_path')
	scratch_path = cfg.get('Output','trash_dir')
	safety_dir = cfg.get('Output', 'safety_dir')
	_dqm_base = cfg.get('Output','dqm_base')
	_ecal_base = cfg.get('Output','ecal_base')
	_lookarea_base = cfg.get('Output','lookarea_base')
	_evd_base = cfg.get('Output','evd_base')
	_evd_eosbase = cfg.get('Output','evd_eosbase')

	_checksum_status = cfg.getboolean('Misc','checksum_status')
	setup_label=cfg.get('Input','setup_label')

	db_config = cfg.get('Bookkeeping', 'db_config')
	db_cred = config.load(db_config)
	connection = cx_Oracle.connect(db_cred.db_user, db_cred.db_pwd,
								   db_cred.db_sid)
	cursor = connection.cursor()

	_streams_with_scalers = cfg.getlist('Streams','streams_with_scalars')
	_streams_to_ecal	  = cfg.getlist('Streams','streams_to_ecal'	 )
	_streams_to_evd	   = cfg.getlist('Streams','streams_to_evd'	  )
	_streams_to_dqm	   = cfg.getlist('Streams','streams_to_dqm'	  )
	_streams_to_lookarea  = cfg.getlist('Streams','streams_to_lookarea' )
	_streams_to_postpone  = cfg.getlist('Streams','streams_to_postpone' )
	_streams_to_ignore	= cfg.getlist('Streams','streams_to_ignore'   )

	_renotify = cfg.getboolean('Misc','renotify')

	max_tier0_transfer_file_size = cfg.getint(
		'Output', 'maximum_tier0_transfer_file_size_in_bytes'
	)

	max_dqm_transfer_file_size = cfg.getint(
		'Output', 'maximum_dqm_transfer_file_size_in_bytes'
	)


	hostname = socket.gethostname()

	#new_path = get_new_path(path, new_path_base)
	#scratch_path = get_new_path(path, _scratch_base)
	rundirs, hltkeys = get_rundirs_and_hltkeys(path, new_path)
	for rundir in rundirs:
		logger.debug("Inspecting `%s' ..." % rundir)
		jsns = sorted(glob.glob(os.path.join(rundir, '*.jsn')))
		if not jsns:
			continue
		rundir_basename = os.path.basename(rundir)
		run_number = int(rundir_basename.replace('run', ''))
		logger.info('********** Run %d **********' % run_number)
		bookkeeper._run_number = run_number
		new_rundir	   = os.path.join(new_path	, rundir_basename)
		rundir_bad	   = os.path.join(rundir	  , 'bad')
		new_rundir_bad   = os.path.join(new_rundir  , 'bad')
		scratch_rundir   = os.path.join(scratch_path, rundir_basename)
		dqm_rundir_open  = os.path.join(_dqm_base   , rundir_basename, 'open')
		dqm_rundir	   = os.path.join(_dqm_base   , rundir_basename)
		ecal_rundir_open = os.path.join(_ecal_base  , rundir_basename, 'open')
		ecal_rundir	  = os.path.join(_ecal_base  , rundir_basename)
		evd_rundir	   = os.path.join(_evd_base   , rundir_basename)
		evd_rundir_open  = os.path.join(_evd_base   , rundir_basename, 'open')
		evd_eosrundir	= os.path.join(_evd_eosbase, rundir_basename)
		lookarea_rundir_open = os.path.join(_lookarea_base  , rundir_basename,
											'open')
		lookarea_rundir	  = os.path.join(_lookarea_base  , rundir_basename)
		run_key = runinfo.get_run_key(run_number)
		if not os.path.exists(scratch_rundir):
			mkdir(scratch_rundir)
			mkdir(os.path.join(scratch_rundir, 'bad'))
		if (not os.path.exists(new_rundir) and
			not run_key == 'TIER0_TRANSFER_OFF'):
			mkdir(new_rundir)
			mkdir(os.path.join(new_rundir, 'bad'))
			logger.info("Opening bookkeeping for run %d ..." % run_number)
			try:
				bookkeeper.open_run(cursor)
				connection.commit()
			except cx_Oracle.IntegrityError:
				logger.warning(
					'Bookkeeping for run %d already open!' % run_number
				)
		appversion = runinfo.get_cmssw_version(run_number)
		if appversion == 'UNKNOWN':
			appversion = get_cmssw_version(run_number)
		#hlt_key = hltkeys[run_number]
		hlt_key = runinfo.get_hlt_key(run_number)
		# Sort JSON files by filename, implying also by lumi.
		jsns.sort()
		# Move the EoR files (ls0000) to the end.
		jsns.sort(key=lambda x: 'EoR' in x)
		logger.info(
			"Processing {count} JSON file(s) in `{folder}':\n".format(
				count=len(jsns), folder=rundir
			) + pprint.pformat([os.path.basename(f) for f in jsns])
		)
		for jsn_file in jsns:
			if ('BoLS' not in jsn_file and
				'EoLS' not in jsn_file and
				'index' not in jsn_file):
				if 'EoR' in jsn_file:
					if run_key == 'TIER0_TRANSFER_OFF':
						maybe_move(jsn_file, scratch_rundir,
								   force_overwrite=True)
					else:
						maybe_move(jsn_file, new_rundir, force_overwrite=True)
					continue
				# Below we used to read the json file
				#settings_textI = open(jsn_file, "r").read()
				#try:
				#	settings = json.loads(settings_textI)
				#except ValueError:
				#	logger.warning("The json file %s is corrupted!" % jsn_file)
				#	maybe_move(jsn_file, new_rundir_bad, suffix='Corrupted')
				#	continue
				#if len(settings['data']) < 5:
				#	logger.warning("Failed to parse `%s'!" % jsn_file)
				#	maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
				#	continue
				#eventsNumber = int(settings['data'][1])
				#fileName = str(settings['data'][3])
				#fileSize = int(settings['data'][4])
				#lumiSection = int(fileName.split('_')[1].strip('ls'))
				##streamName = str(fileName.split('_')[2].strip('stream'))
				#streamName = str(fileName.split('_')[2].split('stream')[1])
				#if ( _checksum_status ):
				#	checksum_int = int(settings['data'][5]) 
				#	checksum = format(checksum_int, 'x').zfill(8)	#making sure it is 8 digits
				#else:
				#	checksum = 0
				safety_subdir = os.path.join(safety_dir, rundir_basename)
				jsn_info=parse_jsn_file(jsn_file, safety_subdir)
				if jsn_info:
					[runnumber, lumiSection, streamName, eventsNumber, fileName, fileSize, checksum, symlink] = jsn_info 
				else:
					continue
				dat_file = os.path.join(rundir, fileName)
				logger.info("The hex format checksum of the file {0} is {1} ".format(dat_file, checksum))
				if streamName in _streams_with_scalers:
					monitor_rates(jsn_file)
				if streamName in _streams_to_postpone:
					continue
				if streamName in _streams_to_dqm:
					## TODO: Use some other temporary directory instead of
					## scratch
					if (fileSize > max_dqm_transfer_file_size):
						logger.warning(
							"`{0}' too large ({1} > {2})! ".format(
								dat_file, fileSize, max_dqm_transfer_file_size
								) +
							"Moving it to bad area with the suffix `TooLarge' ..."
							)
						maybe_move(jsn_file, new_rundir_bad, suffix='TooLarge')
						maybe_move(dat_file, new_rundir_bad, suffix='TooLarge')
					else:
						maybe_move(jsn_file, scratch_rundir)
						maybe_move(dat_file, scratch_rundir)
						jsn_file = jsn_file.replace(rundir, scratch_rundir)
						dat_file = dat_file.replace(rundir, scratch_rundir)
						args = [dat_file, jsn_file, dqm_rundir_open, dqm_rundir]
						dqm_pool.apply_async(move_files, args)
					continue

				if streamName in _streams_to_ecal:
					## TODO: Use some other temporary directory instead of
					## scratch
					maybe_move(jsn_file, scratch_rundir)
					maybe_move(dat_file, scratch_rundir)
					jsn_file = jsn_file.replace(rundir, scratch_rundir)
					dat_file = dat_file.replace(rundir, scratch_rundir)
					args = [dat_file, jsn_file, ecal_rundir_open, ecal_rundir]
					ecal_pool.apply_async(move_files, args)
					continue
				if streamName in _streams_to_lookarea:
					## TODO: Use some other temporary directory instead of
					## scratch
					maybe_move(jsn_file, scratch_rundir)
					maybe_move(dat_file, scratch_rundir)
					jsn_file = jsn_file.replace(rundir, scratch_rundir)
					dat_file = dat_file.replace(rundir, scratch_rundir)
					args = [dat_file, jsn_file, lookarea_rundir_open, lookarea_rundir]
					lookarea_pool.apply_async(move_files, args)
					continue
				if streamName in _streams_to_evd:
					maybe_move(jsn_file, scratch_rundir, force_overwrite=True)
					maybe_move(dat_file, scratch_rundir, force_overwrite=True)
					jsn_file = jsn_file.replace(rundir, scratch_rundir)
					dat_file = dat_file.replace(rundir, scratch_rundir)
					# Dima said they don't need the open area
					args = [dat_file, jsn_file, evd_rundir_open, evd_rundir,
							evd_eosrundir]
					evd_pool.apply_async(copy_move_files, args)
					continue

				if (run_key == 'TIER0_TRANSFER_OFF' or
					streamName in (_streams_with_scalers +
								   _streams_to_ignore)):
					maybe_move(jsn_file, scratch_rundir)
					maybe_move(dat_file, scratch_rundir)
					continue
				if (fileSize > max_tier0_transfer_file_size):
					logger.warning(
						"`{0}' too large ({1} > {2})! ".format(
							dat_file, fileSize, max_tier0_transfer_file_size
						) +
						"Moving it to bad area with the suffix `TooLarge' ..."
					)
					maybe_move(jsn_file, new_rundir_bad, suffix='TooLarge')
					maybe_move(dat_file, new_rundir_bad, suffix='TooLarge')
					continue
				starttime = int(os.stat(dat_file).st_atime)
				stoptime  = int(os.stat(jsn_file).st_ctime)
				maybe_move(dat_file, new_rundir)
				maybe_move(jsn_file, new_rundir)
				## Call the actual inject script
				if eventsNumber == 0:
					number_of_files = 0
				else:
					number_of_files = 1
					args_insert = [
						'./insertFile.pl',
						'--FILENAME'	 , fileName,
						'--FILECOUNTER'  , 0,
						'--NEVENTS'	  , 0,
						'--FILESIZE'	 , 0,
						'--STARTTIME'	, starttime,
						'--STOPTIME'	 , 0,
						'--STATUS'	   , 'open',
						'--RUNNUMBER'	, run_number,
						'--LUMISECTION'  , lumiSection,
						'--PATHNAME'	 , rundir,
						'--HOSTNAME'	 , hostname,
						'--SETUPLABEL'   , setup_label,
						'--STREAM'	   , streamName,
						'--INSTANCE'	 , 1,
						'--SAFETY'	   , 0,
						'--APPVERSION'   , appversion,
						'--APPNAME'	  , 'CMSSW',
						'--TYPE'		 , 'streamer',
						'--CHECKSUM'	 , checksum,
						'--CHECKSUMIND'  , 0,
					]
					args_close = [
						'./closeFile.pl',
						'--FILENAME'	, fileName,
						'--FILECOUNTER' , 0,
						'--NEVENTS'	 , eventsNumber,
						'--FILESIZE'	, fileSize,
						'--STARTTIME'   , starttime,
						'--STOPTIME'	, stoptime,
						'--STATUS'	  , 'closed',
						'--RUNNUMBER'   , run_number,
						'--LUMISECTION' , lumiSection,
						'--PATHNAME'	, new_rundir,
						'--HOSTNAME'	, hostname,
						'--SETUPLABEL'  , setup_label,
						'--STREAM'	  , streamName,
						'--INSTANCE'	, 1,
						'--SAFETY'	  , 0,
						'--APPVERSION'  , appversion,
						'--APPNAME'	 , 'CMSSW',
						'--TYPE'		, 'streamer',
						'--DEBUGCLOSE'  , 2,
						'--CHECKSUM'	, checksum,
						'--CHECKSUMIND' , 0,
					]
					inject_file_path = os.path.join(
						cfg.get('Output', 'inject_base'),
						'{date}-{hostname}.log'.format(
							date=date.today().strftime('%Y%m%d'),
							hostname=hostname,
						)
					)
					with open(inject_file_path, 'a') as inject_file:
						line = ' '.join(map(str, args_insert))
						logger.info(
							"Appending line `%s' to `%s' ..." % (
								line, inject_file_path
							)
						)
						inject_file.write(line + '\n')
						line = ' '.join(map(str, args_close))
						logger.info(
							"Appending line `%s' to `%s' ..." % (
								line, inject_file_path
							)
						)
						inject_file.write(line + '\n')
				try:
					bookkeeper.fill_number_of_files(
						cursor, streamName, lumiSection, number_of_files
					)
					connection.commit()
				except cx_Oracle.IntegrityError:
					print ('WARNING: Failed to insert bookkeeping for ' +
						   'run {0}, stream {1}, ls {2}: #files = {3}').format(
							   run_number, streamName, lumiSection,
							   number_of_files
						   )


		## Move the bad area to new run dir so that we can check for run
		## completeness
		for fname in sorted(glob.glob(os.path.join(rundir_bad, '*.jsn'))):
			try:
				jsn = metafile.File(fname)
				if jsn.type == metafile.Type.MacroMerger:
					dat_path = jsn.path.replace('.jsn', '.dat')
					maybe_move(jsn.path, new_rundir_bad)
					maybe_move(dat_path, new_rundir_bad)
			except ValueError:
				logger.warning("Illegal filename `%s'!" % fname)
	connection.close()
## iterate_main_data()
   
#______________________________________________________________________________
def iterate_private_data():
	_checksum_status = cfg.getboolean('Misc','checksum_status')
	setup_label=cfg.get('Input','setup_label')
	path = cfg.get('Input', 'path')
	#scratch_base = cfg.get('Output','scratch_base')
	#path = cfg.get('Private_Data_Settings', 'path')
	trash_dir = cfg.get('Output', 'trash_dir')
	safety_dir = cfg.get('Output', 'safety_dir')
	valid_pdt_symlinks = cfg.getlist('Private_Data_Settings', 'symlinks')
	
	logger.debug("Inspecting `%s' ..." % path)
	# Check the corresponding pre-transfer directory
	# Get the jsn files in the pre-transfer destination for that subdetector
	dpg_dirs=glob.glob(os.path.join(path,'dpg_*'))
	logger.debug("Looking at {0} for dpg_dirs".format(os.path.join(path,'dpg_*')))
	logger.debug("dpg_dirs are {0}".format(str(dpg_dirs)))
	for dpg_dir in dpg_dirs:
		dpg_base=os.path.basename(dpg_dir)
		logger.debug("Looking at {0} dir, searching {1}".format(dpg_dir,os.path.join(dpg_dir,'*.jsn)')))
		jsns = sorted(glob.glob(os.path.join(dpg_dir, '*.jsn')))
		logger.debug("jsn files are {0}".format(str(jsns)))
		if not jsns:
			continue
		for jsn_file in jsns:
			safety_subdir=os.path.join(safety_dir,dpg_base)
			jsn_info=parse_jsn_file(jsn_file, safety_subdir)
			if jsn_info:
				[runnumber, lumiSection, streamName, eventsNumber, fileName, fileSize, checksum, symlink] = jsn_info
			else:
				continue
			# Make sure the symlink is legit
			if symlink not in valid_pdt_symlinks:
				logger.warning("{0} contains a symlink not accounted for in configuration mapping. Moving it to {1}".format(jsn_file, safety_dir))
				maybe_move(jsn_file, os.path.join(safety_dir, dpg_base), suffix='BadSymlink')
				continue
			EOS_dir = os.path.join( cfg.get('Private_Data_Symlinks', symlink), 'run'+str(runnumber))
			dat_file = os.path.join(path, dpg_dir, fileName)

			# Move stuff to safety dir
			# trash_subdir=os.path.join(trash_dir,symlink,'run'+str(runnumber))
			maybe_move(jsn_file, safety_subdir, force_overwrite=True)
			maybe_move(dat_file, safety_subdir, force_overwrite=True)
			jsn_file=os.path.join(safety_subdir,os.path.split(jsn_file)[1])
			dat_file=os.path.join(safety_subdir,os.path.split(dat_file)[1])

			# Now copy to eos and move to the aux dirs if desired
			aux_path = cfg.get('Private_Data_Auxiliary_Destinations','aux_path_'+symlink)
			aux_dir = os.path.join( aux_path, 'run'+str(runnumber))
			if not aux_path=='':
				aux_dir_open=os.path.join(aux_dir,'open')
				if not os.path.exists(aux_dir):
					os.makedirs(aux_dir)
				if not os.path.exists(aux_dir_open):
					os.makedirs(aux_dir_open)
				args=(dat_file,jsn_file,aux_dir_open,aux_dir,EOS_dir,checksum)
				pdt_pool.apply_async(copy_move_files,args)
			else:
				args=(dat_file,jsn_file,EOS_dir,checksum)
				pdt_pool.apply_async(copy_files,args)
## iterate_private_data()

#______________________________________________________________________________
def get_new_path(path, new_base):
	'''
	Given the path to watch, returns the new path under which the files 
	being transferred are moved.
	'''
	head, tail = os.path.split(path)
	return os.path.join(head, new_base)
## get_new_path()


#______________________________________________________________________________
def mkdir(path):
	logger.info("Making directory `%s' ..." % path)
	os.mkdir(path)
## mkdir()


#______________________________________________________________________________
def get_rundirs_and_hltkeys(path, new_path):
	rundirs, runs, hltkeymap = [], [], {}
	for rundir in sorted(glob.glob(os.path.join(path, 'run*')), reverse=True):
		run_number = int(os.path.basename(rundir).replace('run',''))
		new_rundir = os.path.join(new_path, rundir)
		if eor.Run(new_rundir).is_closed():
			continue
		rundirs.append(rundir)
		runs.append(run_number)
	results = runinfo.get_hlt_keys(runs)
	hltkeys = dict(zip(runs, results))
	rundirs.sort()
	#runnumbers = [r.replace(os.path.join(path, 'run'), '') for r in rundirs]
	if len(runs) == 0:
		logger.info("Found no run directories in '{0}' ... globbed {1}".format(path,os.path.join(path,'run*')))
	else:
		logger.info(
			"Inspecting %d dir(s) in `%s' for runs %s to %s ..." % (
				len(rundirs), path, str(runs[0]), str(runs[-1])
			)
		)
	logger.debug(pprint.pformat(runs))
	logger.debug('HLT keys: ' + format_hltkeys(hltkeys))
	return rundirs, hltkeys
## get_rundirs_and_hltkeys()


#______________________________________________________________________________
def get_cmssw_version(run_number):

	_old_cmssw_version = cfg.get('Misc','old_cmssw_version')
	current_cmssw_version = _old_cmssw_version
	## Sort the first_run -> new_cmssw_version map by the first_run
	##Zeynep's Hack Just so that the Run continues- To BE CORRECTED
	##sorted_rv_pairs = sorted(_first_run_to_new_cmssw_version_map.items(),
	##						 key=lambda x: x[0])
	##for first_run, new_cmssw_version in sorted_rv_pairs:
	##	if first_run <= run_number:
	##		current_cmssw_version = new_cmssw_version
	##	else:
	##		break
	current_cmssw_version = "UNKNOWN"
	return current_cmssw_version
	
## get_cmssw_version()


#______________________________________________________________________________
def monitor_rates(jsn_file):
	fname = jsn_file + 'data'
	basename = os.path.basename(fname)
	try:
		logger.info('Inserting %s in WBM ...' % basename)
		monitorRates.monitorRates(fname)
		delay = get_time_since_modification(fname)
		max_delay = timedelta(
			seconds = cfg.getfloat('Misc', 'seconds_for_wbm_injection')
		)
		if delay < max_delay:
			logger.info(
				'Inserted {0} in WBM with a delay of {1}.'.format(fname, delay)
			)
		else:
			logger.warning(
				'Inserted {0} in WBM with too large of a delay: {1}!'.format(
					fname, delay
				)
			)
	except cx_Oracle.IntegrityError:
		logger.warning('DB record for %s already present!' %  basename)
## monitor_rates


#______________________________________________________________________________
def mock_move_file_to_dir(src, dst, force_overwrite=False, suffix=None,
						  eos=False, src_checksum=False):
	'''
	Prints a message about how it would move the file src to the directory dst
	if this was for real.
	'''
	## Append the filename to the destination directory
	basename = os.path.basename(src)
	if suffix is not None:
		name, extension = os.path.splitext(basename)
		basename = name + suffix + extension
	dst = os.path.join(dst, basename)
	if eos:
		command =  ("xrdcp " + str(src) + " root://eoscms.cern.ch//" +
			str(dst))
		logger.info("I would do: %s" % command)
	else:
	   logger.info("I would do: mv %s %s" % (src, dst))
## mock_move_file_to_dir()

#______________________________________________________________________________
def move_file_to_dir(src, dst_dir, force_overwrite=False, suffix=None,
					 eos=False, src_checksum=False):
	'''
	Moves the file src to the directory dst_dir. Creates dst_dir if it doesn't
	exist.
	'''
	## Append the filename to the destination directory
	src_dir , basename = os.path.split(src)
	if suffix is not None:
		name, extension = os.path.splitext(basename)
		basename = name + suffix + extension
	dst_path = os.path.join(dst_dir, basename)

	if not os.path.exists(src):
		logger.error("Source file `%s' doesn't exist!" % src)
		return
	if os.path.exists(dst_path):
		if os.path.samefile(src, dst_path):
			logger.info(
				"No need to do: mv %s %s, it is the same file." % (
				src, dst_path
				)
			)
			return
		elif force_overwrite:
			logger.info("Overwriting `%s'" % dst_path)
		else:
			raise RuntimeError, "Destination file `%s' exists!" % dst_path
	max_retries=cfg.getint('Misc','max_retries')
	n_retries=0
	while n_retries < max_retries:
		try:
			if eos:
				# do copy to eos
				# need to improve eos error handling
				cmd_cp_to_eos =  ("xrdcp " + str(src) + " root://eoscms.cern.ch//" + str(dst_path))
				logger.info("Running '{0}' (try # {1} / {2}) ...".format(cmd_cp_to_eos,n_retries+1, max_retries))
				result=os.popen(cmd_cp_to_eos).read()
				#if result[0:5]=='error':
				#	raise ValueError('eos error')				
				if src_checksum: # then verify the checksums:
					cmd_get_checksum_info="eos fileinfo {0} --checksum".format(str(dst_path))
					dst_checksum_info=os.popen(cmd_get_checksum_info).read()
					if len(dst_checksum_info.split('\n')) < 2:
						raise ValueError("checksum fileinfo failure")
					dst_checksum = dst_checksum_info.split('\n')[1].split(':')[1].strip()
					if dst_checksum == src_checksum:
						logger.info("Hex checksums match (source xs='{0}', dest xs='{1}')".format(src_checksum, dst_checksum))
					else:
						logger.warning("Hex checksum mismatch: Recorded checksum of source file '{0}' (xs = {1}) disagrees with xrdcp'd EOS file '{2}' (xs = {3})".format(src,src_checksum,dst_path,dst_checksum))
						raise ValueError("hex checksum mismatch")
				#logger.info("Success running %s" % command)
				break
			else:
				if src_checksum: # then verify the checksums:
					cmd_cp="cp {0} {1}".format(src, dst_path)
					logger.info("Running '{0}' (try # {1} / {2}) ...".format(cmd_cp,n_retries+1, max_retries))
					os.system(cmd_cp)
					dst_checksum = format(get_positive_checksum(dst_path),'x').zfill(8)
					if dst_checksum == src_checksum:
						logger.info("Hex checksums match (source xs='{0}', dest xs='{1}')".format(src_checksum, dst_checksum))
						cmd_rm="rm {0}".format(src)
						logger.info("Running '{0}' ...".format(cmd_rm,n_retries+1))
						os.system(cmd_rm)
					else:
						logger.warning("Hex checksum mismatch: Recorded checksum of source file '{0}' (xs = {1}) disagrees with xrdcp'd EOS file '{2}' (xs = {3})".format(src,src_checksum,dst_path,dst_checksum))
						raise ValueError("hex checksum mismatch")
				else:
					# Don't care about the checksum validity, so just do a mv
					logger.info("Running 'mv {0} {1}' (try # {2}) ...".format(src, dst_path,n_retries+1))
					shutil.move(src, dst_path)
				break
		except IOError as error:
			if error.errno == 2 and error.filename == dst_path:
				## Directory dst_dir doesn't seem to exist. Let's create it.
				logger.warning(
					"Failed moving `%s' b/c destination `%s' does not exist. Making it and retrying" % (
						os.path.basename(src), dst_dir
					)
				)
				mkdir_with_parents(dst_dir)
			else:
				logger.error(
					"errno: %d, filename: %s, message: %s" % (
					   error.errno, error.filename, error.strerror
					)
				)
				raise error
		except ValueError as error:
			if error.args[0] == 'hex checksum mismatch':
				# try checking the source file
				logger.info("Calculating checksum of source file")
				src_calculated_checksum = format(get_positive_checksum(src),'x').zfill(8)
				if src_calculated_checksum != dst_checksum:
					logger.warning("Hex checksum mismatch: Calculated checksum of source file '{0}' (xs = '{1}') disagrees with xrdcp'd EOS file '{2}' (xs = '{3}')".format(src,src_calculated_checksum,dst_path,dst_checksum))
				else:
					logger.info("Hex checksums match after calculating that of source (source xs='{0}', dest xs='{1}')".format(src_calculated_checksum, dst_checksum))
					break
			if error.args[0] == 'checksum fileinfo failure':
				logger.warning("Could not get checksum info using EOS (command = '{0}')".format(cmd_get_checksum_info))
			# not actually used at the moment, but needs to be caught properly:
			if error.args[0] == 'eos error':
				eos_error_code=result[7:].split(' ')[0].split('=')
				logger.warning("Error communicating with EOS (error code {0}".format(eos_error_code))
		
		n_retries+=1
## move_file_to_dir()

#______________________________________________________________________________
def move_files(datFile, jsnFile, final_rundir_open, final_rundir, datFileChecksum=False):
	try:
		# first move to open area
		maybe_move(datFile, final_rundir_open)
		maybe_move(jsnFile, final_rundir_open, force_overwrite=True, suffix=None, eos=True, src_checksum=datFileChecksum)
		# then move to the final area
		maybe_move(os.path.join(final_rundir_open,os.path.basename(datFile)),
				   final_rundir, force_overwrite=True, suffix=None, eos=False, src_checksum=datFileChecksum)
		maybe_move(os.path.join(final_rundir_open,os.path.basename(jsnFile)),
				   final_rundir)
	except Exception as e:
		logger.exception(e)
## move_files()

#______________________________________________________________________________
def copy_move_files(datFile, jsnFile, final_rundir_open, final_rundir,
					final_eosrundir, datFileChecksum=False):
	try:
		# first copy or move to the final area with the eos parameter
		maybe_move(datFile, final_eosrundir, force_overwrite=True, suffix=None, eos=True, src_checksum=datFileChecksum)
		maybe_move(jsnFile, final_eosrundir,eos=True)

		# first move to open area in the nfs
		maybe_move(datFile, final_rundir_open,force_overwrite=True, suffix=None, eos=False, src_checksum=datFileChecksum)
		maybe_move(jsnFile, final_rundir_open,eos=False)
		# then move to the final area in the nfs
		maybe_move(os.path.join(final_rundir_open,os.path.basename(datFile)),
				   final_rundir, force_overwrite=True, suffix=None, eos=False, src_checksum=datFileChecksum)
		maybe_move(os.path.join(final_rundir_open,os.path.basename(jsnFile)),
				   final_rundir,eos=False)
	except Exception as e:
		logger.exception(e)
## copy_move_files()

#______________________________________________________________________________
def copy_files(datFile, jsnFile, final_eosrundir, datFileChecksum=False):
	try:
		# copy to the final area with the eos parameter
		maybe_move(datFile, final_eosrundir, force_overwrite=True, suffix=None, eos=True, src_checksum=datFileChecksum)
		maybe_move(jsnFile, final_eosrundir,eos=True)
	except Exception as e:
		logger.exception(e)
## copy_files()

#______________________________________________________________________________
def log_and_exec(args, print_output=False):
	## Make sure all arguments are strings; cast integers.
	args = map(str, args)
	logger.info("Running `%s' ..." % ' '.join(args))
	p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if print_output:
		if out:
			logger.info('STDOUT: ' + str(out))
		if err:
			logger.info('STDERR: ' + str(err))
	return out, err
## log_and_exec()


#______________________________________________________________________________
def log_and_do_not_exec(args, print_output=False):
	## Make sure all arguments are strings; cast integers.
	args = map(str, args)
	log("I would run:\n  %s" % ' '.join(args))
## log_and_do_not_exec()


#______________________________________________________________________________
def need_to_retransfer(out):
	for status in _file_status_list_to_retransfer:
		if status.lower() in out.lower():
			return True
	return False
## need_to_retransfer()


#______________________________________________________________________________
def log(msg, newline=True):
	msg = "%s: %s" % (strftime(), msg)
	if newline:
		logger.info(msg)
	else:
		logger.info(msg)
## log()


#______________________________________________________________________________
def strftime():
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
## strftime()


#______________________________________________________________________________
## http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
def mkdir_with_parents(path):
	'''
	Create a directory, including parents when needed.  No error when exists.
	'''
	try:
		logger.info("Making directory `%s' (incl. parents) ..." % path)
		os.makedirs(path)
	except OSError as exc: # Python >2.5
		if exc.errno == errno.EEXIST and os.path.isdir(path):
			logger.info("Directory `%s' already exists!" % path)
			pass
		else:
			raise
## mkdir_with_parents


#______________________________________________________________________________
def format_hltkeys(hltkeys, max_runs=4, indent=4):
	rows = []
	run_map = invert(hltkeys)
	# t = (<hltkey>, [<run_1>, <run_2>, ..., <run_n>])
	for hltkey, runs in sorted(run_map.items(), key=lambda t: min(t[1])):
		if len(runs) == 1:
			row = hltkey + ': run ' + str(runs[0])
		elif len(runs) < 5:
			row = hltkey + ': runs ' + ', '.join(map(str, runs))
		else:
			row = '{hltkey}: {count} runs between {first} and {last}'.format(
				hltkey=hltkey, count=len(runs), first=min(runs), last=max(runs)
			)
		rows.append(row)
	return ('\n' + indent * ' ').join(rows)
## format_hltkeys


#______________________________________________________________________________
def invert(mapping):
	'''
	Returns an inverse of the given mapping.
	http://stackoverflow.com/questions/483666/python-reverse-inverse-a-mapping
	'''
	inverse_mapping = {}
	for key, value in mapping.iteritems():
		inverse_mapping.setdefault(value, []).append(key)
	return inverse_mapping
## invert_mapping


#______________________________________________________________________________
def get_positive_checksum(path):
	checkSumIni=1
	with open(path, 'r') as fsrc:
		length=16*1024
		while 1:
			buf = fsrc.read(length)
			if not buf:
				break
			checkSumIni=zlib.adler32(buf,checkSumIni)
	return checkSumIni & 0xffffffff
## get_positive_checksum


#______________________________________________________________________________
def parse_jsn_file(jsn_file, safety_subdir):
	_checksum_status = cfg.getboolean('Misc','checksum_status')
	settings_textI = open(jsn_file, "r").read()
	try:
		settings = json.loads(settings_textI)
	except ValueError:
		logger.warning("The json file %s is corrupted!" % jsn_file)
		maybe_move(jsn_file, safety_subdir, suffix='Corrupted', force_overwrite=True)
		return False
	if len(settings['data']) != 10:
		logger.warning("Failed to parse `%s' (malformed) not enough or too many entries in 'data'!" % jsn_file)
		maybe_move(jsn_file, safety_subdir, suffix='Malformed', force_overwrite=True)
		return False
	eventsNumber = int(settings['data'][1])
	fileName = str(settings['data'][3])
	fileSize = int(settings['data'][4])
	runnumber = int(fileName.split('_')[0].strip('run'))
	lumiSection = int(fileName.split('_')[1].strip('ls'))
	streamName = str(fileName.split('_')[2].split('stream')[1])
	if ( _checksum_status ):
		checksum_int = int(settings['data'][5]) 
		checksum = format(checksum_int, 'x').zfill(8)	#making sure it is 8 digits
	else:
		checksum = 0
	symlink = str(settings['data'][9])
	return [runnumber, lumiSection, streamName, eventsNumber, fileName, fileSize, checksum, symlink]
## parse_json_file


#______________________________________________________________________________
def get_time_since_modification(filename):
	'''
	Returns the timedelta object giving the time since the last modification
	of the file given by filename.
	'''
	m_time_stamp = int(os.stat(filename).st_mtime)
	m_utc_date_time = datetime.utcfromtimestamp(m_time_stamp)
	return datetime.utcnow() - m_utc_date_time

#______________________________________________________________________________
if __name__ == '__main__':

   # main(_path)
   main()
