#!/usr/bin/env python
import glob
import getopt
import logging
import os.path
import socket
import sys

from smhook.macroeor import is_run_complete

logging.basicConfig(
    level=logging.INFO, 
    format=r'%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%y-%m-%d %H:%M:%S',
)

def doCompleteRun(paths_to_watch, completeMergingThreshold, nLoopsMax):
   streamsToExclude = ["DQM", "Error", "DQMCalibration",
       "DQMHistograms", "EcalCalibration", "EventDisplay",
       "HLTRates", "L1Rates", "EvDOutput", "EvDOutput2", "LookArea"]
   storeIniArea = "/store/lustre/mergeMacro"
   nLoops = 0
   while nLoops < nLoopsMax:
      nLoops = nLoops + 1
      inputDataFolders = glob.glob(paths_to_watch)
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
          outputEndName = socket.gethostname()

          is_run_complete(inputDataFolder, completeMergingThreshold, 
                          outputEndName, streamsToExclude, storeIniArea)

"""
Main
"""
valid = ['paths_to_watch=', 'debug=', 'threshold=', 'nLoopsMax=', 'help']

usage =  "Usage: testCompleteRun.py --paths_to_watch=<paths_to_watch>\n"
usage += "                          --debug=<0>\n"
usage += "                          --threshold=<1>\n"
usage += "                          --nLoopsMax=<1>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "/store/lustre/transfer/run235849"
completeMergingThreshold = 1.0
nLoopsMax = 1

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--nLoopsMax":
      nLoopsMax = arg
   if opt == "--threshold":
      completeMergingThreshold = float(arg)

if not os.path.exists(paths_to_watch):
   msg = "paths_to_watch folder Not Found: %s" % paths_to_watch
   raise RuntimeError, msg

doCompleteRun(paths_to_watch, completeMergingThreshold, nLoopsMax)
