#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Extracts the list of streams and luminosity sections for a given file name
and copies them to the T0 based on the setup label provided. Verifies the
checksum of the file at the destination. If the copy fails, re-tries N number
of times with NN xsec interval as specified in the configuration.

TODO:
  * implement ability to re-inject without copying (only notification)
  * implement ability to re-inject with copying
  * test the full implementation
'''

import glob
import json
import logging
import os.path
import pprint
import sys
import subprocess, threading
import time

import smhook.config as config
import smhook.injectWorker as injectWorker
import smhook.databaseAgent as databaseAgent
from smhook.elasticSearch import elasticMonitorUpdate

debug=True
logger = logging.getLogger(__name__)

_filename = "run100000_ls0001_streamExpress_StorageManager.dat"
_checksum = 0

if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

#______________________________________________________________________________
def buildcommand(command):
    eos_env={'EOS_MGM_URL':'root://eoscms.cern.ch','KRB5CCNAME':'FILE:/tmp/krb5cc_0'}
    p = subprocess.Popen(command, shell=True, env=eos_env)    
    out, error = p.communicate()
    return out, error, p.returncode

#______________________________________________________________________________
def check_known_returncodes(returncode):
    '''Identifies the known causes of the failure modes and raises the appropriate
    value error
    '''

    error_codes={2: "no such file", 22: "eos auth", 255: "missing source file"}
    if returncode in error_codes:
        logger.exception(returncode)
        raise ValueError(error_codes[returncode])
    else:
        logger.exception(returncode)
        raise ValueError(str(returncode))

#______________________________________________________________________________
def parse_filename(filename):
    '''Extracts the run number, stream name and luminosity section number
    from the given filename of the json file.
    '''
    ## A JSON filename looks for example like this:
    ## run225115_ls0011_streamALCAPHISYM_StorageManager.jsn
    ## run225115_ls0011_streamALCAPHISYM_StorageManager.dat

    root, extention = os.path.splitext(filename)
    tokens = root.split('_')
    skip_message = "Skipping `%s' ..." % filename
    if len(tokens) != 4:
        logger.debug(skip_message)
        return None
    run_token, ls_token, stream_token, sm_token = tokens
    run_len, ls_len, stream_len, sm_len = map(len, tokens)
    if (run_token   [:len('run')   ] != 'run'         or
        ls_token    [:len('ls')    ] != 'ls'          or
        stream_token[:len('stream')] != 'stream'      or
        sm_token                     != 'StorageManager'):
        logger.debug(skip_message)
        return None

    run    = int(run_token   [len('run')   :])
    lumi   = int(ls_token    [len('ls')    :])
    stream =     stream_token[len('stream'):]
    return run, lumi, stream

#______________________________________________________________________________
def lfn_and_pfn(destination,setuplabel,filename):
    '''
    Convert the LFN to PFN
    '''

    ##filename expected is in the form of: runXXX_lsXXX_streamXXX_Tranfser.dat
    ##lfn example: /store/t0streamer/TransferTest/Express/000/282/124
    ##pfn example: /eos/cms/store/t0streamer/TransferTest/Express/000/282/124/run282124_ls0127_streamExpress_StorageManager.dat

    run, lumi, stream = parse_filename(filename)
    lfn = os.path.join(str(destination),str(setuplabel),str(stream),'000',str(run)[:3],str(run)[3:])
    pfn = os.path.join('/eos/cms/',lfn,filename)
    
    logger.info("lfn {0}, pfn {1}, run {2}, strrun {3}".format(lfn, pfn, run, str(run)))

    return lfn, pfn

#______________________________________________________________________________
def eos_makedir(lfn_path):
    '''
    Create a directory in eos including the parents if needed
    '''

    try:
        logger.info("Making directory in eos `%s' (incl. parents) ..." % lfn_path)
        mkdircommand = "eos mkdir -p " + lfn_path
        logger.debug("mkdircommand is {0}".format(mkdircommand))
        out, error, returncode = buildcommand(mkdircommand)
        if returncode != 0:
            check_known_returncodes(returncode)
        return returncode

    except Exception as error:        
        logger.exception(error)        

#______________________________________________________________________________
def compare_checksum(src,dest,checksum,local=False):
    '''
    Compare the checksums of the source file with the destination (or locally)
    '''
    
    if local:
        src_checksum =  get_positive_checksum(src)
        if src_checksum == checksum:
            logger.info("Hex checksums match (source xs='{0}', dest xs='{1}')".format(checksum, src_checksum))
            return True
        else:
            logger.warning("Hex checksum mismatch: Recorded checksum of source file '{0}' (xs = {1}) disagrees with the locally computed checksum (xs = {2})".format(src,checksum,src_checksum))
            return False

    else:
        cmd_get_checksum_info="eos fileinfo {0} --checksum".format(str(dest))
        out, error, returncode = buildcommand(cmd_get_checksum_info)
        
        if returncode != 0:
            check_known_returncodes(returncode)

        else:
            eos_checksum = out.split('\n')[1].split(':')[1].strip()
            if eos_checksum == checksum:
                logger.info("Hex checksums match (source xs='{0}', dest xs='{1}')".format(checksum, eos_checksum))
                return True
            else:
                logger.warning("Hex checksum mismatch: Recorded checksum of source file '{0}' (xs = {1}) disagrees with xrdcp'd EOS file (xs = {2})".format(src,checksum,eos_checksum))
                return False

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


#______________________________________________________________________________
def copy_to_t0(src,pfn_path):
    
    '''Does the operation of xrdcp copy to eos destination.
    checks for the failure modes.
    '''

    try:
        # Copy silently and overwrite the existing file if it exists.
        # Here add the tag from P5 as well to trace it in eos side
        copycommand = ("xrdcp -f -s " + str(src) + " root://eoscms.cern.ch//" + str(pfn_path))
        logger.info("Running `%s' ..." % copycommand)
        out, error, returncode = buildcommand(copycommand)
        if returncode != 0:
            check_known_returncodes(returncode)
        return returncode

    except Exception as error:
        logger.exception(error)


#______________________________________________________________________________
def copyFile(file_id, fileName, checksum, path, destination, setup_label, esServerUrl='',esIndexName='', max_retries=1):
    # Main interface to the daemon for actually copying the files
    # Checksum can be falsebe provided, either file_id or fileName can be false
    if not file_id and not fileName:
        # Not enough info to do the transfer
        logger.warning("copyWorker.copyFile received too little information about the file")
        return
    if not checksum==0 or not file_id or not fileName:
        [file_id, fileName, checksum] = getFileInfo(file_id,fileName,checksum)
    if not checksum==0 or not file_id or not fileName:
        line = "copyWorker.copyFile could not recover enough information about file from the database"
        if not file_id:
            line += " (filename = '{0}')".format(fileName)
        elif not fileName:
            line += " (file ID = {0})".format(file_id)
        logger.warning(line)
        if not (file_id < 0 ): 
            return
        
    lfn_path, pfn_path = lfn_and_pfn(destination, setup_label, fileName)
    makedir_status = eos_makedir(lfn_path)
    logger.debug("Return code for the eos directory creation was {0}".format(makedir_status))

    #Record the transfer start time before the retry loop, so the retries affect the rate
    if (file_id >= 0) : 
        transferstart = injectWorker.recordTransferStart(file_id)
        logger.warning("Transfer start time record status is {0}".format(transferstart))
        setlfn = injectWorker.recordTransferPath(file_id,lfn_path)
        logger.warning("Transfer path status in the db is {0}".format(setlfn))

    n_retries = 0
    logger.warning("You are at the {0} retry out of {1} retries".format(n_retries, max_retries))
    while n_retries < max_retries:
        copy_status = copy_to_t0(path,pfn_path)
        
        if copy_status !=0:
            n_retries+=1
            time.sleep(30)
            continue
        if checksum != 0 and int(checksum) != 0:
            checksum_comparison_remote = compare_checksum(path,pfn_path,checksum,local=False)
            if checksum_comparison_remote is True:
                if (file_id >= 0) : injectWorker.recordTransferComplete(file_id)
                return
            else:
                checksum_comparison_local = compare_checksum(path,pfn_path,checksum,local=True)
                if checksum_comparison_local is True:
                    # Local checksum comparison is fine, need to retry in 30 seconds
                    n_retries+=1
                    time.sleep(30)
                    continue
                else:
                    # File is corrupted locally, daemon will move it to the bad area
                    if (file_id >= 0) : injectWorker.recordCorruptedTransfer(file_id)
                    logger.warning("The file is corrupted and is moved to the bad area. Retries are stopped!")
                    return
        else:
            if (file_id >= 0) : injectWorker.recordTransferComplete(file_id)
            logger.info("The file {0} is successfully transfered".format(fileName))
            if not (esServerUrl=='' or esIndexName==''):
                monitorData = [int(time.time()*1000.), 2]
                elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, fileName, 5)            
            return
        n_retries+=1

#______________________________________________________________________________
def getFileInfo(file_id, fileName, checksum):
    if not fileName and not file_id:
        return [file_id, fileName, checksum]
    if file_id<0:
        where_clause = " WHERE FILENAME='"+fileName+"'" 
    else:
        where_clause = " WHERE FILE_ID='"+str(file_id)+"'" 
    query = "SELECT FILE_ID, FILENAME, CHECKSUM FROM CMS_STOMGR.FILE_TRANSFER_STATUS "+where_clause
    result = databaseAgent.runQuery('file_status', query, fetch_output=True)

    logger.info("{0}".format(result))

    if result and len(result[0])==3:
        [file_id,fileName,checksum] = result[0]
        logger.info("file_id {0}, filename {1}, checksum {2}".format(file_id,fileName,checksum))
    return [file_id,fileName,checksum]
#______________________________________________________________________________
def main():
    
    logger.info('*** Starting the copying of the file %s to T0 ... ***' %  _filename)

    ##in the hook we would need to call this function as:
    #t0copy._filename = XXXX.dat
    #t0copy._checksum = XXXX
    #t0copy.main()

    fileName = _filename
    checksum = _checksum
    run_number = 100000
    lumiSection = 1
    streamName = 'Express'
    
    dest = "/store/t0streamer/"
    setup_label = 'TransferTest'

    ## Put in an identifier to tell T0 to repack the file or not
    inject_into_T0 = True
    if 'TransferTest' in setup_label: #or add other special flags we define
        inject_into_T0 = False

    # mark the file as new

    result=injectWorker.insertFile(fileName, run_number, lumiSection, streamName, checksum, inject_into_T0)
    file_id = -1
    new_file_path = "/opt/python/smhook/"+fileName
    copyFile(file_id, fileName, checksum, new_file_path, dest, setup_label, esServerUrl='', esIndexName='' ,max_retries=1) 

## main

#______________________________________________________________________________
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s in %(module)s: %(message)s')
    logger.info("Running `%s' ..." % ' '.join(sys.argv))
    if len(sys.argv) > 1:
        for filename_as_str in sys.argv[1:]:
            _filename = str(filename_as_str)
            _checksum = 1
            main()
    else:
        main()
    import user




