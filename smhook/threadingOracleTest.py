from multiprocessing.pool import ThreadPool
import cx_Oracle
import datetime
import random
import string
import threading
import time
import multiprocessing
import smhook.databaseAgent

db_config = smhook.databaseAgent.db_config
cxn_name = "CMS_STOMGR";
the_cxn = cx_Oracle.connect(db_config[cxn_name]['user'], db_config[cxn_name]['pwd'], db_config[cxn_name]['sid'], threaded = True)

def make_threading_test_table():
  # Generates a dummy table for the test
  cursor=the_cxn.cursor()
  q1="""
    CREATE TABLE THREADING_TEST_TABLE (
      ID_NUMBER   NUMBER(27)     NOT NULL,
      RANDOM_TEXT VARCHAR2(1000) NOT NULL,
      PRIMARY KEY (ID_NUMBER)
    )"""
  q2="""
    CREATE SEQUENCE SM_THREADING_TEST_SEQ
    MINVALUE 1 MAXVALUE 9999999999999999
    START WITH 1
    INCREMENT BY 1
    CACHE 20
  """
  print q1
  print q2
  cursor.execute(q1)
  cursor.execute(q2)
  the_cxn.commit()

def insert_crap(numdigits, numchars):
  # Generate some random text and put it in the dummy table
  digits = "".join( [random.choice(string.digits) for i in xrange(numdigits)] )
  chars = "".join( [random.choice(string.letters) for i in xrange(numchars)] )
  random_text = digits + chars

  cursor=the_cxn.cursor()
  cursor.execute("""
    INSERT INTO THREADING_TEST_TABLE (ID_NUMBER, RANDOM_TEXT)
    VALUES (CMS_STOMGR.SM_THREADING_TEST_SEQ.NEXTVAL, '{0}')
  """.format(random_text))
  the_cxn.commit()
  return True

def test_threads(n_threads, n_queries, n_iterations):
  # Distribute a total number of queries among threads
  the_thread_pool = ThreadPool(n_threads)
  cursor = the_cxn.cursor()
  cursor.execute("""SELECT COUNT(*) FROM THREADING_TEST_TABLE""")
  result=cursor.fetchone()
  rows_before=result[0]
  t1=int(round(time.time() * 1000000))/1000.
  iteration=1
  arguments = [8,15]
  while iteration <= n_iterations:
    print "iteration {0}".format(iteration)
    for n_query in range(0,n_queries):
      the_thread_pool.apply_async(insert_crap, arguments)
    iteration += 1
  the_thread_pool.close()
  the_thread_pool.join()

  t2=int(round(time.time() * 1000000))/1000.
  cursor.execute("""SELECT COUNT(*) FROM THREADING_TEST_TABLE""")
  result=cursor.fetchone()
  rows_after=result[0]
  print "test threads: {0} threads, {1} queries/iteration, {2} iterations, {3} total rows inserted, {4} milliseconds ".format(n_threads, n_queries, n_iterations, rows_after - rows_before, t2-t1)






















