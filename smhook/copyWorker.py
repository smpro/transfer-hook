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
import subprocess

import smhook.config as config
logger = logging.getLogger(__name__)

_filename = "run000000_ls0001_streamExpress_StorageManager.dat"

#______________________________________________________________________________
def buildcommand(command):

    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
    lfn = str(destination)+'/'+str(setuplabel)+'/'+str(stream)+'/000/'+str(run)[:3]+'/'+str(run)[3:]+'/'
    pfn = '/eos/cms/'+lfn+filename
    
    logger.info("lfn {0}, pfn {1}, run {2}, strrun {3}".format(lfn, pfn, run, str(run)))

    return lfn, pfn

#______________________________________________________________________________
def eos_makedir(lfn_path):
    '''
    Create a directory in eos including the parents if needed
    '''

    try:
        logger.info("Making directory in eos `%s' (incl. parents) ..." % lfn_path)
        mkdircommand = "eos mkdir -p root://eoscms.cern.ch//" + lfn_path
        logger.info("mkdircommand is {0}".format(mkdircommand))
        out, error, returncode = buildcommand(mkdircommand)
        if returncode != 0:
            check_known_returncodes(returncode)

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
        out, error, returncode = buildcommand(cmd_get_checksum)
        
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
        copycommand = ("xrdcp -f -s" + str(src) + " root://eoscms.cern.ch//" + str(pfn_path))
        logger.info("Running `%s' ..." % copycommand)
        out, error, returncode = buildcommand(copycommand)
        if returncode != 0:
            check_known_returncodes(returncode)
        return returncode

    except Exception as error:
        logger.exception(error)


#______________________________________________________________________________
def insert_file_to_db(src,setup_label,status,inject_flag):
    '''Dummy function to temporarily print out what it shoudl be doing'''
    logger.info("I will be injecting information to the DB about file {0} the setup label is {1}, the status to be injected is {2} the injection flag is {3}".format(src,setup_label,status,inject_flag))

#______________________________________________________________________________
def main():
    
    logger.info('*** Starting the copying of the file %s to T0 ... ***' %  _filename)

    ##in the hook we would need to call this function as:
    #t0copy._filename = XXXX.dat
    #t0copy._checksum = XXXX
    #t0copy.main()

    src      = _filename
    checksum = _checksum
    
    #status = check_file_from_db(src)
    
    #if (status > "NEW"):
        # this would mean the file is already is copied, so need to re-copy.
    #    break
    

    dest = "/store/t0streamer/"
    setup_label = 'TransferTest'


    ## Put in an identifier to tell T0 to repack the file or not
    inject_flag = 1
    if setup_label == ['TransferTest']: #or add other special flags we define
        inject_flag = 0

    # mark the file as new
    insert_file_to_db(src,setup_label,"NEW",inject_flag)



    lfn_path, pfn_path = lfn_and_pfn(dest,setup_label,src)
    eos_makedir(lfn_path)

    n_retries = 0
    max_retries = 1

    while n_retries < max_retries:

        copy_status = copy_to_t0(src,pfn_path)
        
        if copy_status !=0:
            n_retries+=1
            continue
        
        if checksum != 0: continue
        
        # compare the json checksum to the eos checksum
        if(compare_checksum(src,pfn_path,jsn_checksum,local=False)):
            #everything is ok, now you can fill the db
            ## PUT THE FILE IN THE DB with the inject flag
            insert_file_to_db(src,setup_label,status,inject_flag)
            break
        else:
            if(compare_checksum(src,pfn_path, jsn_checksum,local=True)):
                #this means you need to retry, first sleep for 30 seconds
                #And then it will automatically will go into the retry
                os.system("sleep 30s")
            else:
                ##move the file somewhere "bad", perhaps define a new folder
                insert_file_to_db(src,setup_label,status,inject_flag,False)
                logger.warning("The file is corrupted and is moved to the bad area. The retries are stopped!")
                break
                
        n_retries+=1


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




