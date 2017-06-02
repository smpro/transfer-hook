#!/usr/bin/env python
import os.path,argparse
import getopt, sys,os,fcntl
import cx_Oracle
import bookkeeper as bookkeeper
import config as config

def main():

    parser = argparse.ArgumentParser(description='Fix total lumi count by hand')
    parser.add_argument("-r", "--runnumber",dest="runnumber"   , help="number of the run you want to check", type=int)
    parser.add_argument("-l", "--luminumber",dest="luminumber" , help="total lumi you want to set", type=int)

    args = parser.parse_args()

    if args.runnumber > 0 and args.luminumber>-1 :
        filename = os.path.join(config.DIR, '.db_rcms_cred.py') # production DB
        cred = config.load(filename)
        connection = cx_Oracle.connect(cred.db_user, cred.db_pwd, cred.db_sid)
        cursor = connection.cursor()
        bookkeeper.setup()
        bookkeeper._run_number = args.runnumber
        bookkeeper.close_run(args.luminumber,cursor)
        connection.commit()
        connection.close()

main()
