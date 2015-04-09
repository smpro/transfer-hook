#!/bin/env  python
# -*- coding: utf-8 -*-
'''
TODO:
    * Output lines to a log file - use a different one than the production
    * Cross-check with existing lines in the production files

USAGE:
    ./rehook.py <runnumber> <stream> <lumi1> [<lumi2> [<lumi3> ...]]

EXAMPLE
    ./rehook.py 238929 A 28 29 31 32 33 35

Example lines:
./insertFile.pl --FILENAME run238929_ls0005_streamA_StorageManager.dat --FILECOUNTER 0 --NEVENTS 0 --FILESIZE 0 --STARTTIME 1427295140 --STOPTIME 0 --STATUS open --RUNNUMBER 238929 --LUMISECTION 5 --PATHNAME /store/lustre/mergeMacro/run238929 --HOSTNAME srv-C2C07-16 --SETUPLABEL Data --STREAM A --INSTANCE 1 --SAFETY 0 --APPVERSION CMSSW_7_3_2_patch2 --APPNAME CMSSW --TYPE streamer --CHECKSUM 0 --CHECKSUMIND 0
./closeFile.pl --FILENAME run238929_ls0005_streamA_StorageManager.dat --FILECOUNTER 0 --NEVENTS 38 --FILESIZE 1871063 --STARTTIME 1427295140 --STOPTIME 1427295140 --STATUS closed --RUNNUMBER 238929 --LUMISECTION 5 --PATHNAME /store/lustre/transfer/run238929 --HOSTNAME srv-C2C07-16 --SETUPLABEL Data --STREAM A --INSTANCE 1 --SAFETY 0 --APPVERSION CMSSW_7_3_2_patch2 --APPNAME CMSSW --TYPE streamer --DEBUGCLOSE 2 --CHECKSUM 0 --CHECKSUMIND 0

'''
import datetime
import logging
import os
import os.path
import socket
import sys

from optparse import OptionParser

from smhook.metafile import MacroMergerFile
from smhook.runinfo import runinfo

logger = logging.getLogger(__name__)
_inject_base = '/store/global/log' ## production
# _inject_base = '/tmp'              ## test

def main():
    logging.basicConfig(
        level=logging.INFO,
        filename='rehook.log',
        format=r'%(asctime)s %(name)s %(levelname)s: %(message)s',
        )
    datestring = datetime.date.today().strftime('%Y%m%d')
    hostname = socket.gethostname()
    filename = '{0}-{1}-rehook.log'.format(datestring, hostname)
    inject_file_path = os.path.join(_inject_base, filename)
    logger.info("Using `%s' for injection." % inject_file_path)
    with open(inject_file_path, 'a') as inject_file:
        for jsn in get_jsn_files():
            logging.info('Re-hooking %s.' % jsn.basename)
            appversion = runinfo.get_cmssw_version(jsn.run)
            hlt_key = runinfo.get_hlt_key(jsn.run)
            start_time = jsn.get_data_file_mtime()
            stop_time  = jsn.get_data_file_mtime()
            args_insert = [
                './insertFile.pl',
                '--FILENAME'     , jsn.data_file_name,
                '--FILECOUNTER'  , 0,
                '--NEVENTS'      , 0,
                '--FILESIZE'     , 0,
                '--STARTTIME'    , start_time,
                '--STOPTIME'     , 0,
                '--STATUS'       , 'open',
                '--RUNNUMBER'    , jsn.run,
                '--LUMISECTION'  , jsn.ls,
                '--PATHNAME'     , jsn.dirname,
                '--HOSTNAME'     , hostname,
                '--SETUPLABEL'   , 'Data',
                '--STREAM'       , jsn.stream,
                '--INSTANCE'     , 1,
                '--SAFETY'       , 0,
                '--APPVERSION'   , appversion,
                '--APPNAME'      , 'CMSSW',
                '--TYPE'         , 'streamer',
                '--CHECKSUM'     , 0,
                '--CHECKSUMIND'  , 0,
            ]
            args_close = [
                './closeFile.pl',
                '--FILENAME'    , jsn.data_file_name,
                '--FILECOUNTER' , 0,
                '--NEVENTS'     , jsn.accepted,
                '--FILESIZE'    , jsn.data_file_size,
                '--STARTTIME'   , start_time,
                '--STOPTIME'    , stop_time,
                '--STATUS'      , 'closed',
                '--RUNNUMBER'   , jsn.run,
                '--LUMISECTION' , jsn.ls,
                '--PATHNAME'    , jsn.dirname,
                '--HOSTNAME'    , hostname,
                '--SETUPLABEL'  , 'Data',
                '--STREAM'      , jsn.stream,
                '--INSTANCE'    , 1,
                '--SAFETY'      , 0,
                '--APPVERSION'  , appversion,
                '--APPNAME'     , 'CMSSW',
                '--TYPE'        , 'streamer',
                '--DEBUGCLOSE'  , 2,
                '--CHECKSUM'    , 0,
                '--CHECKSUMIND' , 0,
            ]
            append(what=args_insert, to=inject_file)
            append(what=args_close , to=inject_file)
        logger.info('Done.')

def get_jsn_files():
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()
    run_number, stream_name = args[:2]
    dir_name = '/store/lustre/transfer/run' + run_number
    jsn_files = []
    for lumi_section_number in args[2:]:
        jsn_files.append(
            MacroMergerFile.from_tokens(
                dir_name, run_number, stream_name, lumi_section_number
                )
            )
    return jsn_files

def make_option_parser():
    usage = ('Usage: %prog [options] <runnumber> <stream> <ls1> ' +
             '[<ls2> ... <lsn>]')
    parser = OptionParser(usage=usage)
    return parser
    
def append(what, to):
    '''
    Appends a line created from argument list "what" to a file object "to".
    '''
    line = ' '.join(map(str, what))
    logger.debug("Appending `%s' ..." % line)
    to.write(line + '\n')

if __name__ == '__main__':
    main()
