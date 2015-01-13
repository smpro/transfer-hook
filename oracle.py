#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cx_Oracle
import logging
import imp

logger = logging.getLogger(__name__)

## defines db_sid, db_user and db_pwd
_db_config = '.db.conf.py'
_db_config = '.db.int2r.stomgr_tier0_r.cfg.py'
_db_config = '.db.int2r.stomgr_w.cfg.py'
_db_config = '.db.rcms.stomgr_w.cfg.py'



#______________________________________________________________________________
def main():
    '''Main entry point for execution'''
    setup()
    logger.debug('Connecting to %s@%s ...' % (_db_user, _db_sid))
    db = cx_Oracle.connect(_db_user, _db_pwd, _db_sid)
    cursor = db.cursor()
    dump_queries(cursor)
    db.close()
## main


#______________________________________________________________________________
def setup():
    global _db_user, _db_pwd, _db_sid
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s in %(module)s: %(message)s')
    logger.debug("Loading DB config `%s'" % _db_config)
    cfg = imp.load_source('db_config', _db_config)
    _db_user = cfg.db_user
    _db_pwd = cfg.db_pwd
    _db_sid = cfg.db_sid
## setup


#______________________________________________________________________________
def dump_queries(cursor):
    # dump_runs2(cursor)
    #dump_runs(cursor)
    # dump_open_runs(cursor)
    # dump_streams2(cursor)
    dump_closed_lumis_and_filecount(cursor,
                                    stream='A', runnumber=231309)
    dump_closed_lumis_and_filecount(cursor,
                                    stream='NanoDST', runnumber=231309)
    #dump_closed_lumis_and_filecount(cursor,
                                    #stream='Calibration', runnumber=231198)
    dump_closed_runs(cursor, 231309)
    # dump_closed_runs(cursor, 231024)
    # dump_closed_runs(cursor, 231027)
## dump_queries


#______________________________________________________________________________
def dump_streams2(cursor):
    query = 'select RUNNUMBER, STREAM, LUMISECTION, FILECOUNT from cms_stomgr.streams2'
    cursor.execute(query)
    for result in cursor:
        print 'run, stream, lumi, filecount:', result
## dump_streams2


#______________________________________________________________________________
def dump_runs2(cursor):
    query = 'select RUNNUMBER, HOSTNAME, N_LUMISECTIONS from cms_stomgr.runs2'
    print query
    cursor.execute(query)
    for result in cursor:
        print 'run, hostname, lumis:', result
## dump_runs2 


#______________________________________________________________________________
def dump_runs(cursor):
    query = 'select RUNNUMBER, STATUS, N_LUMISECTIONS from cms_stomgr.runs where runnumber >= 230984'
    print query
    print '%6s   %6s   %6s' % ('run', 'status', 'lumis')
    print '------------------------------------'
    cursor.execute(query)
    for result in cursor:
        print '%6d   %6d   %6d' % result
## dump_runs


#______________________________________________________________________________
def dump_open_runs(cursor):
    query = 'select RUNNUMBER, STATUS, N_LUMISECTIONS from cms_stomgr.runs where runnumber >= 230984'
    print query
    print '%6s   %6s   %6s' % ('run', 'status', 'lumis')
    print '------------------------------------'
    cursor.execute(query)
    for result in cursor:
        run, status, lumis = result
        if status == 1:
            print '%6d   %6d   %6d' % result
## dump_runs


#______________________________________________________________________________
def dump_closed_runs(cursor, runnumber):
    query = """SELECT a.runnumber, MAX(a.n_lumisections)
               FROM CMS_STOMGR.runs a
               WHERE a.runnumber = %d
                 AND a.status = 0
               GROUP BY a.runnumber
               HAVING COUNT(*) = MAX(a.n_instances)
               """ % runnumber
    cursor.execute(query)
    for result in cursor:
        print 'run, lumis:', result
## dump_closed_runs


#______________________________________________________________________________
def dump_closed_lumis_and_filecount(cursor, stream='A', runnumber=231127):
    logger.debug(
        'Looking for closed lumis in stream %s of run %d ...' % (stream,
                                                                 runnumber)
    )
    sql = """SELECT a.runnumber, a.stream, a.lumisection, SUM(a.filecount)
             FROM CMS_STOMGR.streams a
             INNER JOIN CMS_STOMGR.runs b ON
               b.runnumber = a.runnumber AND
               b.instance = a.instance
             WHERE a.runnumber = :RUN
             AND a.stream = :STREAM
             AND a.lumisection > :LUMI
             GROUP BY a.runnumber, a.stream, a.lumisection
             HAVING ( COUNT(*) = ( SELECT COUNT(*)
                                   FROM CMS_STOMGR.runs c
                                   WHERE c.runnumber = a.runnumber ) AND
                      COUNT(*) = ( SELECT MAX(c.n_instances)
                                   FROM CMS_STOMGR.runs c
                                   WHERE c.runnumber = a.runnumber ) )
             OR ( COUNT(*) = SUM(CASE
                                   WHEN b.status = 1 THEN 1000
                                   WHEN b.n_lumisections < a.lumisection THEN 0
                                   ELSE 1
                                 END) AND
                  COUNT(*) = ( SELECT SUM(CASE
                                            WHEN c.status = 1 THEN 1000
                                            WHEN c.n_lumisections < a.lumisection THEN 0
                                            ELSE 1
                                          END)
                                      FROM CMS_STOMGR.runs c
                                      WHERE c.runnumber = a.runnumber ) )
             """
    logger.debug("Using SQL query: `%s'" % sql)
    cursor.prepare(sql)
    cursor.execute(None, {'RUN': runnumber, 'STREAM': stream, 'LUMI': 0})
    results = []
    for result in cursor:
        results.append(result)
    results.sort()
    for result in results:
        print 'run, stream, lumi, filecount:', result
## dump_closed_lumis_and_filecount


#insert = "insert into cms_stomgr.streams2 values (2, 3, 'A', 1, 1, TO_DATE('2014-09-05 12:28:52','YYYY-MM-DD HH24:MI:SS'), 1)"
#cursor.execute(insert)
#db.commit()


if __name__ == '__main__':
    main()
    import user

