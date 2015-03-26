#!/bin/env python
# -*- coding: utf-8 -*-
'''
Calculate the average bandwidth of transfer during a run given
    * the date and time of the start
    * the date and time of the end
    * the total size in GB
as command-line arguments.  For example, for run 238751

    ./bandwidth.py "Thu Mar 26 10:44:40 2015" "Thu Mar 26 15:00:24 2015" 793.46

'''
import sys
from datetime import datetime, timedelta

def main():
    if len(sys.argv) != 4:
        print "Require 3 arguments, received %d!:" % len(sys.argv) - 1,
        print sys.argv[1:]
        print_usage()
        sys.exit(1)
    start_string, stop_string, size_string = sys.argv[1:]
    start_time = datetime.strptime(start_string, '%a %b %d %H:%M:%S %Y')
    stop_time  = datetime.strptime(stop_string , '%a %b %d %H:%M:%S %Y')
    dtime = stop_time - start_time
    size = float(size_string)
    bandwidth = size * 1024 / total_seconds(dtime)
    print '%.2f MB/s' % bandwidth

def print_usage():
    print 'USAGE: ./bandwidth.py <start> <stop> <size>'
    print 'For example: ./bandwidth.py "Thu Mar 26 10:44:40 2015" ' +\
        '"Thu Mar 26 15:00:24 2015" 958.13'

def total_seconds(td):
    return td.seconds + td.days * 24 * 3600

if __name__ == '__main__':
    main()