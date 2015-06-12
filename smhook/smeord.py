#!/bin/env python
# -*- coding: utf-8 -*-
'''
A simple daemon to run eor as a service.
'''
import sys
import os
import socket
import signal
import logging
import logging.config

from multiprocessing import Process

import smhook.config
import smhook.eor

from smhook.daemon import Daemon

PIDFILE = '/var/run/smeord.pid'

## Use these for production
STDOUT = '/dev/null'
STDERR = '/dev/null'
if os.path.exists("/opt/python/smhook/config/smeord_priority.conf"):
        CONFIGFILE = '/opt/python/smhook/config/smeord_priority.conf'
else:
    CONFIGFILE = '/opt/python/smhook/config/smeord.conf'


logger = logging.getLogger(__name__)

class Service(Daemon):
    running = True
    def cleanup(self, signum = None, frame = None):
        self.logger.debug('Cleaning up ...')
        smhook.eor.terminate()
        self.delpid()
        self.running = False
    def run(self):
        self.logger.info(
            'Using config file(s): %s ...' % ', '.join(
                smhook.config.config.filenames
            )
        )
        self.logger.debug("Calling smhook.eor.run() ...")
        try:
            smhook.eor.run()
        except Exception as e:
            self.logger.critical(
                "Exiting due to an exception in smhook.eor.run()!"
            )
            self.logger.exception(e)
            raise e

def main():
    logging.config.fileConfig(CONFIGFILE)
    smhook.config.init(CONFIGFILE)
    daemon = Service(PIDFILE, stdout=STDOUT, stderr=STDERR)

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
