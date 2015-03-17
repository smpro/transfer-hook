#!/bin/env python
'''
TODO:
    * Catch exceptions in child processes and log them
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

#CONFIGFILE = '/opt/python/smhook/smhookd.conf'
#STDOUT = '/dev/null'
#STDERR = '/opt/python/smhook/smhookd.out'
#PIDFILE = '/var/run/smhookd.pid'

CONFIGFILE = '/opt/python/smhook/smhookd_test.conf'
STDOUT = '/dev/null'
STDERR = '/opt/python/smhook/test/smhookd.out'
PIDFILE = '/opt/python/smhook/test/smhookd.pid'

logger = logging.getLogger(__name__)

class SMHookD(Daemon):
    running = True
    def cleanup(self, signum = None, frame = None):
        self.logger.debug('Cleaning up ...')
        if hasattr(self, 'children'):
            self.logger.debug('Terminating child processes ...')
            for proc in self.children:
                if proc is not None:
                    self.logger.debug('Terminating {0} ...'.format(proc))
                    try:
                        proc.terminate()
                    except OSError, err:
                        errs = str(err)
                        if errs.find("No such process") > 0:
                            self.logger.warning(
                                '{0} not running!'.format(proc)
                            )
                    except AttributeError, err:
                        if not proc:
                            self.logger.warning(
                                '{0} not running!'.format(proc)
                            )
                        else:
                            raise err
                else:
                    self.logger.warning('A child process is "None"!')
            #map(Process.terminate, self.children)
        self.delpid()
        self.running = False

    def run(self):
        self.children = []
        self.logger.debug("Creating a child process for smhook.hello.run() ...")
        self.children.append(Process(target=smhook.hello.run, args = []))
        self.logger.debug("Creating a child process for smhook.eor.run() ...")
        self.children.append(Process(target=smhook.eor.run, args = []))
        self.logger.debug(
            "Creating a child process for smhook.watchAndInject.main() ..."
        )
        self.children.append(
            Process(target=smhook.watchAndInject.main, args = [])
        )
        self.logger.debug('Starting child processes asynchronously ...')
        map(Process.start, self.children)
        self.logger.debug('Waiting for child processes to finish ...')
        map(Process.join, self.children)
        self.logger.debug("Finished.")

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
