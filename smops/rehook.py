#!/bin/env  python
# -*- coding: utf-8 -*-
'''
TODO:
    * Use real start time and stop time
    * Output lines to a log file - use a different one than the production
    * Cross-check with existing lines in the production files
'''
import datetime
import os
import sys
import socket

from smhook.metafile import MacroMergerFile
from smhook.runinfo import runinfo

_inject_base = '/store/global/log'
_test_file = '/store/lustre/transfer/run239145/run239145_ls0001_streamA_StorageManager.jsn'
def main():
    print sys.argv
    hostname = socket.gethostname()
    inject_file_path = os.path.join(
        _inject_base,
        '{date}-{hostname}.log'.format(
            date=datetime.date.today().strftime('%Y%m%d'),
            hostname=hostname,
        )
    )
    jsn = MacroMergerFile(_test_file)
    appversion = runinfo.get_cmssw_version(jsn.run)
    starttime = 123
    stoptime = 123
    hlt_key = '/dummy/hlt/key'
    args_insert = [
        './insertFile.pl',
        '--FILENAME'     , jsn.file_name,
        '--FILECOUNTER'  , 0,
        '--NEVENTS'      , 0,
        '--FILESIZE'     , 0,
        '--STARTTIME'    , starttime,
        '--STOPTIME'     , 0,
        '--STATUS'       , 'open',
        '--RUNNUMBER'    , jsn.run,
        '--LUMISECTION'  , jsn.ls,
        '--PATHNAME'     , jsn.path,
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
        '--FILENAME'    , jsn.file_name,
        '--FILECOUNTER' , 0,
        '--NEVENTS'     , jsn.accepted,
        '--FILESIZE'    , jsn.file_size,
        '--STARTTIME'   , starttime,
        '--STOPTIME'    , stoptime,
        '--STATUS'      , 'closed',
        '--RUNNUMBER'   , jsn.run,
        '--LUMISECTION' , jsn.ls,
        '--PATHNAME'    , jsn.path,
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
    line = ' '.join(map(str, args_insert))
    print line
    line = ' '.join(map(str, args_close))
    print line
    print 'FIN'

if __name__ == '__main__':
    main()
