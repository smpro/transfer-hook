#!/bin/env python
'''
A simple daemon to run watchAndInject as a service.
'''
import sys
import os
import socket
import signal
import logging

from multiprocessing import Process

import smhook.config
import smhook.hello
import smhook.eor
import smhook.watchAndInject

from smhook.daemon import Daemon

CONFIGFILE = '/opt/python/smhook/config/smhookd.conf'
STDOUT = '/dev/null'
STDERR = '/opt/python/smhook/smhookd.out'
PIDFILE = '/var/run/smhookd.pid'

#CONFIGFILE = '/opt/python/smhook/config/smhookd_test.conf'
#STDOUT = '/dev/null'
#STDERR = '/opt/python/smhook/test/smhookd.out'
#PIDFILE = '/opt/python/smhook/test/smhookd.pid'

logger = logging.getLogger(__name__)

class SMHookD(Daemon):
    running = True
    def cleanup(self, signum = None, frame = None):
        self.logger.debug('Cleaning up ...')
        self.delpid()
        self.running = False
    def run(self):
        self.logger.debug("Calling smhook.watchAndInject.main() ...")
        try:
            smhook.watchAndInject.main()
        except Exception as e:
            self.logger.critical(
                "Exiting due to an exception in smhook.watchAndInject.main()!"
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
