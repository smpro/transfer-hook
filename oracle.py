# -*- coding: utf-8 -*-
import cx_Oracle
## defines db_sid, db_user and db_pwd
# execfile('.db.conf.py')
## defines db_sid, db_user and db_pwd
execfile('.db.int2r.stomgr_tier0_r.cfg.py')
db = cx_Oracle.connect(db_user, db_pwd, db_sid)
cursor = db.cursor()

#insert = "insert into cms_stomgr.streams2 values (2, 3, 'A', 1, 1, TO_DATE('2014-09-05 12:28:52','YYYY-MM-DD HH24:MI:SS'), 1)"
#cursor.execute(insert)
#db.commit()

query = 'select RUNNUMBER, STREAM, LUMISECTION, FILECOUNT from cms_stomgr.streams2'
cursor.execute(query)
for result in cursor:
    print 'run, stream, lumi, filecount:', result

query = 'select RUNNUMBER, HOSTNAME from cms_stomgr.runs2'
cursor.execute(query)
for result in cursor:
    print 'run, hostname:', result

db.close()