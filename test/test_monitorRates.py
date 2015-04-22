import logging
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

import os.path
import smhook.monitorRates as rates

def main():
    rates.dropTestTables()
    rates.makeTestTables()
    MYBASE = '/nfshome0/veverka/lib/python/smhook/test/rates_test'
    for fname in ['run231133_ls0001_streamL1Rates_mrg-c2f13-35-01.jsndata',
                  'run231133_ls0001_streamHLTRates_mrg-c2f13-35-01.jsndata']:
        fpath = os.path.join(MYBASE, fname)
        logging.info('Injecting %s in the DB ...' % fpath)
        rates.monitorRates(fpath)
    rates.outputTestTables()

if __name__ == '__main__':
    main()