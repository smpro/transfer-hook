# -*- coding: utf-8 -*-
'''
Test if the locked files appear as nonexistent to other processes.

USAGE:
    Terminal 1 do:

        ssh bu-c2e18-09-01
        cd /nfshome0/veverka/daq/mwgr/transfer-hook/test
        ## Remove all old lock files
        for f in /store/lustre/test/veverka/*.lock; do
            echo "rm $f"
            rm $f
        done
        ls -l /store/lustre/test/veverka/

    The output should look like this:

        total 0

    There are now lock files present, which we want. Start the process 1:

        python lock.py

    The output should look similar to:

        2014-11-14 15:24:34,407: /store/lustre/test/veverka/test.lock does not exist. Creating it ...
        2014-11-14 15:24:34,408: Locking /store/lustre/test/veverka/test.lock ...
        2014-11-14 15:24:34,408: Writing into /store/lustre/test/veverka/test.lock ...
        Hit enter to conitnue ...

    The execution has now stopped and is waiting for input. The process
    is holding a lock to the file test.lock.  We can inspect the file in
    terminal 2:

        ssh bu-c2e18-11-01
        cd /nfshome0/veverka/daq/mwgr/transfer-hook/test
        ls -l /store/lustre/test/veverka/

    You should see the file is present:

        total 1
        -rw-r--r-- 1 veverka zh 46 Nov 14 15:16 test.lock

    And it contains a line written by process 1:

        $ cat /store/lustre/test/veverka/test.lock
        15:24:34: This is the first line in the file.

    Now we start process 2 to test if Python can see the file or not:

        $ python lock.py
        2014-11-14 15:33:04,903: /store/lustre/test/veverka/test.lock exists
        2014-11-14 15:33:04,903: Locking /store/lustre/test/veverka/test.lock ...

    ... and it does. It seems that Python is well aware of the file test.lock,
    independent if it's locked or not.
'''

import fcntl
import os
import logging as log
import datetime

fname = '/store/lustre/test/veverka/test.lock'
log.basicConfig(level=log.DEBUG,
                format='%(asctime)s: %(message)s')

#_______________________________________________________________________________
def main():
    #check_existence_lock_and_write()
    lock_check_size_and_write()

#_______________________________________________________________________________
def check_if_exists_lock_and_write():
    if os.path.exists(fname):
        log.info('%s exists. Opening it for appending ...' % fname)
        with open(fname, 'a') as fdesc:
            lock_and_write(fdesc, 'This is another line.')
    else:
        log.info('%s does not exist. Creating it ...' % fname)
        with open(fname, 'w') as fdesc:
            lock_and_write(fdesc, 'This is the first line in the file.')
    log.info('Closed %s.' % fname)

#_______________________________________________________________________________
def lock_check_size_and_write():
    log.info('Opening %s for appending ...' % fname)
    with open(fname, 'a') as fdesc:
        log.info('Locking %s ...' % fdesc.name)
        fcntl.flock(fdesc, fcntl.LOCK_EX)
        log.info('Checking size of %s ...' % fdesc.name)
        if os.path.getsize(fdesc.name) == 0:
            log.info('%s is empty. Writing first line ...' % fdesc.name)
            write(fdesc, 'This is the first line in the file.')
        else:
            log.info('%s is non-empty. Writing another line ...' % fdesc.name)
            write(fdesc, 'This is another line.')
    log.info('Closed %s.' % fname)

#_______________________________________________________________________________
def lock_and_write(fdesc, msg):
    log.info('Locking %s ...' % fdesc.name)
    fcntl.flock(fdesc, fcntl.LOCK_EX)
    write(fdesc, msg)
    fcntl.flock(fdesc, fcntl.LOCK_UN)

#_______________________________________________________________________________
def write(fdesc, msg):
    log.info('Writing into %s ...' % fdesc.name)
    fdesc.write('{0}: {1}\n'.format(get_strftime_now(), msg))
    fdesc.flush()
    raw_input('Hit enter to conitnue ...\n')


#_______________________________________________________________________________
def get_strftime_now():
    now = datetime.datetime.now()
    return now.strftime("%H:%M:%S")

#_______________________________________________________________________________
if __name__ == '__main__':
    main()