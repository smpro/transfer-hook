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

log.basicConfig(level=log.DEBUG,
                format='%(asctime)s: %(message)s')

def main():
    fname = '/store/lustre/test/veverka/test.lock'

    if os.path.exists(fname):
        log.info('%s exists' % fname)
        with open(fname, 'a') as myfile:
            write_with_lock(myfile, 'This line is appended to an existing file.')
    else:
        log.info('%s does not exist. Creating it ...' % fname)
        with open(fname, 'w') as myfile:
            write_with_lock(myfile, 'This is the first line in the file.')
    log.info('Closed %s.' % fname)

def write_with_lock(fd, msg):
    log.info('Locking %s ...' % fd.name)
    fcntl.flock(fd, fcntl.LOCK_EX)
    log.info('Writing into %s ...' % fd.name)
    fd.write('{0}: {1}\n'.format(get_strftime_now(), msg))
    fd.flush()
    raw_input('Hit enter to conitnue ...\n')
    fcntl.flock(fd, fcntl.LOCK_UN)

def get_strftime_now():
    now = datetime.datetime.now()
    return now.strftime("%H:%M:%S")

if __name__ == '__main__':
    main()