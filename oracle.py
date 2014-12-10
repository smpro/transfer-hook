#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cx_Oracle
## defines db_sid, db_user and db_pwd
# execfile('.db.conf.py')
## defines db_sid, db_user and db_pwd
#execfile('.db.int2r.stomgr_tier0_r.cfg.py')
execfile('.db.rcms.stomgr_w.cfg.py')
print db_user, db_sid

#______________________________________________________________________________
def main():
    '''Main entry point for execution'''
    db = cx_Oracle.connect(db_user, db_pwd, db_sid)
    cursor = db.cursor()
    dump_queries(cursor)
    db.close()
## main


#______________________________________________________________________________
def dump_queries(cursor):
    # dump_runs2(cursor)
    # dump_runs(cursor)
    # dump_streams2(cursor)
    # dump_closed_lumis_and_filecount(cursor)
    dump_closed_runs(cursor, 231018)
    dump_closed_runs(cursor, 231024)
    dump_closed_runs(cursor, 231027)
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
    query = 'select RUNNUMBER, HOSTNAME, N_LUMISECTIONS from cms_stomgr.runs where runnumber > 226485'
    print query
    cursor.execute(query)
    for result in cursor:
        print 'run, hostname, lumis:', result
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
def dump_closed_lumis_and_filecount(cursor):
    query = '''SELECT a.stream, a.lumisection, SUM(a.filecount) 
        FROM CMS_STOMGR.streams2 a 
        WHERE    a.runnumber = 225115 
             AND a.stream = 'A' 
             AND a.lumisection > 0
        GROUP BY a.stream, a.lumisection 
        HAVING COUNT(*) = ( SELECT MAX(b.n_instances) 
                            FROM CMS_STOMGR.runs2 b 
                            WHERE b.runnumber = 225115 )
            OR COUNT(*) = ( SELECT COUNT(*) 
                            FROM ( SELECT b.n_lumisections,
                                          b.n_instances,
                                    COUNT(*) OVER (PARTITION BY b.status) status_count
                            FROM CMS_STOMGR.runs2 b
                            WHERE b.runnumber = 225115
                            AND b.status = 0
                          )
                          WHERE n_lumisections >= a.lumisection
                          AND n_instances = status_count )
        '''
    # https://github.com/dmwm/T0/blob/master/src/python/T0/WMBS/Oracle/RunLumiCloseout/FindClosedLumis.py#L52
    sql = """SELECT a.runnumber
             FROM CMS_STOMGR.runs a
             WHERE a.runnumber = 225115
             GROUP BY a.runnumber
             HAVING COUNT(*) = MAX(a.n_instances)
             """
    # cursor.execute(query)
    cursor.execute(sql)
    for result in cursor:
        print 'stream, lumi, filecount:', result
## dump_closed_lumis_and_filecount   


#insert = "insert into cms_stomgr.streams2 values (2, 3, 'A', 1, 1, TO_DATE('2014-09-05 12:28:52','YYYY-MM-DD HH24:MI:SS'), 1)"
#cursor.execute(insert)
#db.commit()


if __name__ == '__main__':
    main()
    import user

