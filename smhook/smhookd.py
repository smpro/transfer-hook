#!/bin/env python
# -*- coding: utf-8 -*-
'''
A simple daemon to run hook as a service.
TODO: Log the config file used.
'''
import sys
import os
import socket
import signal
import logging
import logging.config

from multiprocessing import Process

import smhook.config
import smhook.hook

from smhook.daemon import Daemon

PIDFILE = '/var/run/smhookd.pid'
KRB5_CONFIG = '/nfshome0/smpro/confidential/krb5.conf.srv-C2C03-22'

# Use these for production
STDOUT = '/dev/null'
STDERR = '/dev/null'
CONFIGFILE = '/opt/python/smhook/config/smhookd.conf'

### Use these for testing
#STDOUT = '/dev/null'
#STDERR = '/tmp/smhookd.out'
#CONFIGFILE = '/opt/python/smhook/config/smhookd_test.conf'

logger = logging.getLogger(__name__)

class SMHookD(Daemon):
    running = True
    def cleanup(self, signum = None, frame = None):
        self.logger.debug('Cleaning up ...')
        self.delpid()
        self.running = False
    def run(self):
        self.logger.info(
            'Using config file(s): %s ...' % ', '.join(
                smhook.config.config.filenames
            )
        )
        ## Temporary hack to make xrdcp work for EvD
        self.logger.info(
            'Setting environment KRB5_CONFIG=%s ...' % KRB5_CONFIG
        )
        os.environ['KRB5_CONFIG'] = KRB5_CONFIG
        self.logger.debug("Calling smhook.hook.main() ...")
        try:
            smhook.hook.main()
        except Exception as e:
            self.logger.critical(
                "Exiting due to an exception in smhook.hook.main()!"
            )
            self.logger.exception(e)
            raise e

def main():
    logging.config.fileConfig(CONFIGFILE)
    smhook.config.init(CONFIGFILE)
    daemon = SMHookD(PIDFILE, stdout=STDOUT, stderr=STDERR)

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'kill' == sys.argv[1]:
            daemon.kill()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        elif 'status' == sys.argv[1]:
            daemon.status()
        else:
            print "Unknown command `%s'" % sys.argv[1]
            print_usage()
            sys.exit(2)
        sys.exit(0)
    else:
        print_usage()
        sys.exit(2)

def print_usage():
    print "Usage: %s start|stop|kill|restart|status" % sys.argv[0]

if __name__ == "__main__":
    main()
