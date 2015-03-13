#!/bin/env python
# -*- coding: utf-8 -*-
'''
Logs a friendly message every given time interval.

USAGE: ./hello.py
'''
import logging
import logging.config
import signal
import time
import smhook.config

CONFIGFILE = '/opt/python/smhook/hello.conf'
logger = logging.getLogger(__name__)
running = True

def main():
    logging.config.fileConfig(CONFIGFILE)
    smhook.config.init(CONFIGFILE)
    run()

def run():
    logger.info('Started ...')
    cfg = smhook.config.config
    message = cfg.get('hello', 'message')
    seconds_to_sleep = cfg.getint('hello', 'seconds_to_sleep')
    signal.signal(signal.SIGTERM, terminator)
    iteration = 0
    while running:
        if iteration % seconds_to_sleep == 0: 
            logger.info(message)
            iteration = 0
        iteration += 1
        time.sleep(1)
    logger.info('... done! Exiting with great success ...')

def terminator(signum, frame):
    global running
    logger.info('Terminating ...')
    running = False


if __name__ == '__main__':
    main()