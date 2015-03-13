#!/bin/env python
import sys
import os
import socket
import signal
import logging

from multiprocessing import Process

import smhook.config
import smhook.hello

from smhook.daemon import Daemon

CONFIGFILE = '/opt/python/smhook/smhookd.conf'
PIDFILE = '/var/run/smhookd.pid'
STDOUT = '/opt/python/smhook/smhookd.out'
STDERR = '/opt/python/smhook/smhookd.out'

logger = logging.getLogger(__name__)


class SMHookD(Daemon):
    running = True

    def cleanup(self, signum = None, frame = None):
        self.logger.debug('Cleaning up ...')
        if hasattr(self, 'children'):
            map(Process.terminate, self.children)
        self.delpid()
        self.running = False

    def run(self):
        self.logger.info("Calling smhook.hello.run() asynchronously ...")
        self.children = []
        self.children.append(Process(target=smhook.hello.run, args = []))
        map(Process.start, self.children)
        map(Process.join, self.children)
        self.logger.info("Finished.")

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
