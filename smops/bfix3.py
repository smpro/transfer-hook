#!/usr/bin/env python
import os.path
import getopt, sys,os,fcntl
import cx_Oracle
import bookkeeper as bookkeeper
import config as config

def main():

    filename = os.path.join(config.DIR, '.db_rcms_cred.py') # production DB
    cred = config.load(filename)
    connection = cx_Oracle.connect(cred.db_user, cred.db_pwd, cred.db_sid)
    cursor = connection.cursor()
    bookkeeper.setup()
    bookkeeper._run_number = run_number
    bookkeeper.fill_number_of_files(cursor, stream_name, ls, 1)
    connection.commit()
    connection.close()

if __name__ == '__main__':

    valid = ['run_number=', 'stream_name=', 'ls=', 'help'] 
    usage =  "Usage: bfix3.py --run_number=<run_number>\n"
    usage += "                          --stream_name=<0>\n"   
    usage += "                          --ls=<1>\n"
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],'',valid)
    except getopt.GetoptError, ex:
        print usage
        print str(ex)
        sys.exit(1)
    
    for opt, arg in opts:
        if opt == "--help":
            print usage
            sys.exit(1)
        if opt == "--run_number":
            run_number = arg
        if opt == "--stream_name":
            stream_name = arg
        if opt == "--ls":
            ls = arg

    main()
