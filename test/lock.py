# -*- coding: utf-8 -*-
'''
Test the locking behaviour.
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