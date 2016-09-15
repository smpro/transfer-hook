#!/bin/env python

# Last modified by Dylan G. Hsu on 2015-05-29 :: dylan.hsu@cern.ch

import os,sys,socket
import shutil
import time,datetime
import cx_Oracle
import json
import logging
import signal
#import multiprocessing

import smhook.config

# Hardcoded Config file to be used, is defined below:
# We read from production DB no matter what (in either case)
# but for testing, write to integration DB only
debug=False
#myconfig = os.path.join(smhook.config.DIR, '.db_rates_integration.py')
myconfig = os.path.join(smhook.config.DIR, '.db_rates_production.py')
cxn_timeout = 60*60 # Timeout for database connection in seconds
num_retries = 5
query_timeout = 1.5 #Timeout for individual queries in seconds

logger = logging.getLogger(__name__)
# For debugging purposes, initialize the logger to stdout if running script as a standalone
if debug == True:
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

# Load the config
logger.info('Using config: %s' % myconfig)
execfile(myconfig)

# Establish DB connections as module globals
# This allows persistent database connections
global cxn_exists, cxn_db, cxn_timestamp, cxn_names
cxn_names = ['trigger_read', 'hlt_rates_write', 'l1_rates_write', 'l1_rate_types_read'] # put in config file later
#temporary
db_usernames = {'trigger_read': trigger_db_login,, 'hlt_rates_write': hlt_rates_db_login, 'l1_rates_write': l1_rates_db_login, 'l1_rate_types_read': l1_rate_type_db_login}
db_passwords = {'trigger_read': trigger_db_pwd, 'hlt_rates_write': hlt_rates_db_pwd, 'l1_rates_write': l1_rates_db_pwd, 'l1_rate_types_read': l1_rate_type_db_pwd}
db_sids      = {'trigger_read': trigger_db_sid, 'hlt_rates_write': hlt_rates_db_sid, 'l1_rates_write': l1_rates_db_sid, 'l1_rate_types_read': l1_rate_type_db_sid}
cxn_exists = {}
cxn_db = {}
cxn_timestamp = {}
for cxn_name in cxn_names:
    cxn_exists[cxn_name]=False
    cxn_timestamp[cxn_name]=0
    cxn_db[cxn_name]=False
    try:
        cxn_db[cxn_name] = cx_Oracle.connect(db_usernames[cxn_name], db_passwords[cxn_name], db_sids[cxn_name])
        cxn_timestamp[cxn_name] = int(time.time())
        cxn_exists[cxn_name] = True
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        logger.error('Error connecting to database "{0}": {1}'.format(cxn_name, returnErrorMessage(error.code)

def returnErrorMessage(code):
    if code==1017:
        return 'Bad credentials'
    else:
        return 'Unrecognized error ({0})'.format(code)

def useConnection(cxn_name):
    global cxn_exists, cxn_db, cxn_timestamp, cxn_names
    if cxn_name not in cxn_names:
        return False
    is_fresh_cxn = int(time.time()) - cxn_timestamp[cxn_name]
    if not cxn_exists[cxn_name] or not is_fresh_cxn[cxn_name]:
        if cxn_exists:
            cxn_db[cxn_name].close()
            cxn_exists[cxn_name]=False
            logger.info('Database connection "{0}" has expired. Making a new one...'.format(cxn_name)
        retries=0
        while not cxn_exists[cxn_name] and retries<num_retries:
            try:
                logger.debug('Try #1 to make a new database connection to "{0}"'.format(cxn_name))
                cxn_db[cxn_name] = cx_Oracle.connect(db_usernames[cxn_name], db_passwords[cxn_name], db_sids[cxn_name])
                cxn_timestamp[cxn_name] = int(time.time())
                cxn_exists[cxn_name] = True
                logger.debug('Successfully reconnected to database "{0}"'.format(cxn_name))
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                logger.error('Error connecting to database "{0}". Reason: {2}'.format(cxn_name, returnErrorMessage(error.code)
                retries+=1
    if not cxn_exists[cxn_name]:
        return False
    else:
        return cxn_db[cxn_name]

class TimeoutError(Exception):
    pass
def timeoutHandler(signum, frame):
    raise TimeoutError()
def timeout(func, args=(), kwargs={}, timeout_duration=1, default=None):
    # set the timeout handler
    signal.signal(signal.SIGALRM, timeoutHandler) 
    signal.alarm(timeout_duration)
    try:
        result = func(*args, **kwargs)
    except TimeoutError as exc:
        result = default
    finally:
        logger.error('Timeout {0} s exceeded calling function {1}'.format(timeout_duration, func.__name__)
        signal.alarm(0)
    return result

def query(cxn_name, query, custom_timeout=0):
    #Interface for passing queries to the database agent
    logger.debug('Passing a query to database connection "{0}": "{1}"'.format(cxn_name, query.replace('\n', ' ').replace('\r', ''))
    the_cxn = useConnection(cxn_name)
    if the_cxn==False:
        logger.error('Could not run query, unable to connect to "{0}"'.format(cxn_name))
        return False
    if custom_timeout!=0:
        the_timeout = custom_timeout
    else:
        the_timeout = query_timeout
    args=[the_cxn, query]
    ran_query=False
    retries=0
    while ran_query==False and retries<num_retries:
        result = timeout(executeQuery, args, timeout_duration=the_timeout, default=False)
    if result==False:
        return False
    else:
        return result
def executeQuery(the_cxn, query):
    #Internal function for actually executing the query
    cursor=the_cxn.cursor()
    cursor.execute(query)
    result=cursor.fetchall()
    return result
