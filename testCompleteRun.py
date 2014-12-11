#!/usr/bin/env python
import os, time, sys, getopt, fcntl
import shutil
import json
import glob
import multiprocessing
from multiprocessing.pool import ThreadPool
import logging
import thread
import datetime
import fileinput
import socket
from macroeor import is_run_complete

def doCompleteRun(paths_to_watch, completeMergingThreshold, debug):
   # Maximum number with pool option (< 0 == always)
   nWithPollMax = -1
   # Maximum number of threads to be allowed with the pool option
   nThreadsMax  = 50
   # Number of loops
   nLoops = 0
   while 1:
      thePool = ThreadPool(nThreadsMax)
      
      nLoops = nLoops + 1
      inputDataFolders = glob.glob(paths_to_watch)
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  outputEndName = socket.gethostname()

	  is_run_complete(debug, inputDataFolder, completeMergingThreshold, outputEndName)

"""
Main
"""
valid = ['paths_to_watch=', 'debug=', 'threshold=', 'help']

usage =  "Usage: testCompleteRun.py --paths_to_watch=<paths_to_watch>\n"
usage += "                          --debug=<0>\n"
usage += "                          --threshold=<1>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "/store/lustre/mergeMacro/run230509"
completeMergingThreshold = 1.0
debug = 10

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--debug":
      debug = arg
   if opt == "--threshold":
      completeMergingThreshold = arg

if not os.path.exists(paths_to_watch):
   msg = "paths_to_watch folder Not Found: %s" % paths_to_watch
   raise RuntimeError, msg

doCompleteRun(paths_to_watch, completeMergingThreshold, debug)
