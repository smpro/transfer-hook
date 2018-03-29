#!/usr/bin/env python
import os.path,argparse
import getopt, sys,os,fcntl
import cx_Oracle
import bookkeeper as bookkeeper
import config as config

def main():

    parser = argparse.ArgumentParser(description='Fix total lumi count by hand')
    parser.add_argument("-r", "--runnumber"   ,dest="runnumber"  ,default=-1, help="number of the run you want to check", type=int)
    parser.add_argument("-tl", "--totallumi"  ,dest="totallumi"  ,default=-1, help="total lumi you want to set", type=int)

    parser.add_argument("-s",  "--stream"     , dest="stream"    , help="stream name you want to correct", type=str)
    parser.add_argument("-sl", "--streamlumi" , dest="streamlumi",default=-1, help="stream lumi you want to correct", type=int)
    parser.add_argument("-sf",  "--streamfill", dest="streamfill",default=-1, help="stream lumi fill you want to correct, 0 or 1", type=int)

    parser.add_argument("-f", "--fill", dest="fill", action='store_true' , help="determines whether you want to update or fill for the first time, if you specify it, it will fill")

    args = parser.parse_args()

    filename = os.path.join(config.DIR, '.db_rcms_cred_master.py') # production DB with write access

    if args.runnumber > 0 and args.totallumi >-1 :
        bookkeeper._run_number = args.runnumber
        bookkeeper.close_run(args.totallumi,cursor)
        connection.commit()
        connection.close()

    if args.runnumber>0 and args.streamlumi>-1 and args.streamfill>-1:
        bookkeeper._run_number = args.runnumber
        if args.fill:
            print "Filling the table"
            bookkeeper.fill_number_of_files(cursor, args.stream, args.streamlumi, args.streamfill)
        else:
            print "Updating the table"
            bookkeeper.update_number_of_files(cursor, args.stream, args.streamlumi, args.streamfill)

        connection.commit()
        connection.close()

main()
