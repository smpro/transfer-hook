#!/usr/bin/env python

#############################################################
################# Managed by puppet !! ######################
#############################################################

import glob
import getopt
import logging
import os.path
import socket
import sys

from smhook.runinfo import RunInfo
from datetime import datetime, timedelta
from smhook.macroeor import is_run_complete

global runinfo
runinfo = RunInfo(os.path.join('/opt/python/smhook/config', '.db.omds.runinfo_r.cfg.py'))


logging.basicConfig(
    level=logging.INFO,
    format=r'%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%y-%m-%d %H:%M:%S',
)


def doCompleteRun(thePaths_to_watch,theStoreIniArea,completeMergingThreshold,nLoopsMax):
   streamsToExclude = ["DQM", "Error", "DQMCalibration",
       "DQMHistograms", "EcalCalibration", "EventDisplay",
       "HLTRates", "L1Rates", "EvDOutput", "EvDOutput2", "LookArea"]
   nLoops = 0
   while nLoops < nLoopsMax:
      nLoops = nLoops + 1
      inputDataFolders = glob.glob(thePaths_to_watch)
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = os.path.join(paths_to_watch,inputDataFolders[nf])
          outputEndName = socket.gethostname()

          is_run_complete(inputDataFolder, completeMergingThreshold, 
                          outputEndName, streamsToExclude, theStoreIniArea)

def stop_time(run_number):
   stop_time_string = runinfo.get_stop_time(run_number)
   if stop_time_string == 'UNKNOWN':
      stop_time = None
   else:
      time_string_format = '%m/%d/%y %I:%M:%S %p %Z'
      stop_time = datetime.strptime(stop_time_string, time_string_format)
      return stop_time

def start_time(run_number):
   start_time_string = runinfo.get_start_time(run_number)
   if start_time_string == 'UNKNOWN':
      start_time = None
   else:
      time_string_format = '%m/%d/%y %I:%M:%S %p %Z'
      start_time = datetime.strptime(start_time_string, time_string_format)
      return start_time

def get_runs(input_path):
   runs = []
   dirnames = glob.glob(os.path.join(input_path, 'run*'))
   dirnames.sort()
   for dirname in dirnames:
      try:
         run_number = int(os.path.basename(dirname).replace('run',''))
         run_key = runinfo.get_run_key(run_number)
         if run_key == 'TIER0_TRANSFER_OFF':
            continue
         if glob.glob(os.path.join(dirname,'*ls0000*TransferEoR*.jsn')):
            continue
         if stop_time(run_number) is None:
            continue
         runs.append(run_number)
      except ValueError:
         print "Skipping ", dirname
   return runs


def stalled_bu(runnumber,thePaths_to_watch,theStoreIniArea,stream):

   miniEOR_dir=thePaths_to_watch+'/run'+str(runnumber)
   ini_dir=theStoreIniArea+'/run'+str(runnumber)

   ini_expr = ini_dir + "/stream" + stream + '/data/run{0}_ls0000_stream{1}_*.ini'.format(runnumber,stream)
   ini_files = glob.glob( ini_expr )
   miniEOR_expr = miniEOR_dir + '/run{0}_ls0000_MiniEoR_*.jsn'.format(runnumber)
   miniEOR_files = glob.glob( miniEOR_expr )

   if len(miniEOR_files)==0:
      print 'No miniEOR files (searched "' + miniEOR_expr + '")'
      return
   if len(ini_files)==0:
      print 'No ini files (searched "' + ini_expr + '")'
      return

   miniEOR_BUs=[]
   ini_BUs=[]
   stalled_BUs=[]
   for file in miniEOR_files:
      pieces=os.path.basename(file).split('.')[0].split('_')
      miniEOR_BUs.append(pieces[3])
   for file in ini_files:
      pieces=os.path.basename(file).split('.')[0].split('_')
      ini_BUs.append(pieces[3])
   if len(miniEOR_files) < len(ini_files):
      stalled_BUs=ini_BUs
      for BU in miniEOR_BUs:
         stalled_BUs.remove(BU)
   return stalled_BUs

################################################################################


"""
Main
"""
valid = ['paths_to_watch=', 'iniArea=', 'help']
usage  =  "Usage: testCompleteRun.py --paths_to_watch=</store/lustre/transfer>\n"
usage +=  "                          --iniArea=</store/lustre/mergeMacro>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "/store/lustre/transfer"
storeIniArea   = "/store/lustre/mergeMacro"
completeMergingThreshold = 1.0
nLoopsMax = 1

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--iniArea":
      storeIniArea = arg

open_runs = get_runs(paths_to_watch)

for run in open_runs:
   full_path = os.path.join(paths_to_watch,'run'+str(run))
   if not os.path.exists(full_path):
      msg = "Folder Not Found: %s" % full_path
      raise RuntimeError, msg
   print "\n \n"
   print "Checking Run: ", full_path
   print "The menu used: ", runinfo.get_hlt_key(run)
   print "The run started at:", start_time(run), "stopped at: ",stop_time(run) 
   doCompleteRun(full_path, storeIniArea, completeMergingThreshold, nLoopsMax)
   #print "Stalled BUs: ",stalled_bu(run,paths_to_watch,storeIniArea,'*')
   if(paths_to_watch == "/store/lustre/transfer"):
      print "Stalled BUs: ",stalled_bu(run,paths_to_watch,storeIniArea,'NanoDST')
      print "If stalled BUs are found, please log into the bu:", stalled_bu(run,paths_to_watch,storeIniArea,'NanoDST'), " and execute: python /opt/merger/cmsDataFlowCheckFolder.py --paths_to_watch=/fff/output/run" + str(run) ,"for further information"
   else:
      print "Stalled BUs: ",stalled_bu(run,paths_to_watch,storeIniArea,'EcalCalibration')
      print "If stalled BUs are found, please log into the bu:", stalled_bu(run,paths_to_watch,storeIniArea,'EcalCalibration'), " and execute: python /opt/merger/cmsDataFlowCheckFolder.py --paths_to_watch=/fff/output/run" + str(run) ,"for further information"

