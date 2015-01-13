# -*- coding: utf-8 -*-
'''
Basic test of the transfer.hook.runinfo module.
Jan Veverka, 12 January 2015, veverka@mit.edu

USAGE:
    $ python runinfo_test.py

Expected output:
Run 229221
  appversion: CMSSW_7_2_1
  HLT key: /cdaq/special/LS1/ECR/v1/HLT/V4
  run key: UNKNOWN
Run 232062
  appversion: CMSSW_7_2_3
  HLT key: /cdaq/special/LS1/MWGR9/HLT/V11
  run key: UNKNOWN
Runs 229221 and 232062
  appversions: ['CMSSW_7_2_1', 'CMSSW_7_2_3']
  HLT keys: ['/cdaq/special/LS1/ECR/v1/HLT/V4', '/cdaq/special/LS1/MWGR9/HLT/V11']
  run keys: ['UNKNOWN', 'UNKNOWN']

'''
import os
import logging
import transfer.hook.runinfo as runinfo
import transfer.hook.config as config

logging.basicConfig(level = logging.INFO)

ri = runinfo.RunInfo(
    config.Config(
        db_config_file = '/nfshome0/veverka/lib/python/transfer/hook/.db.omds.runinfo_r.cfg.py',
    )
)
print 'Run 229221'
print '  appversion:', ri.get_cmssw_version(229221)
print '  HLT key:'   , ri.get_hlt_key(229221)
print '  run key:'   , ri.get_run_key(229221)
print 'Run 232062'
print '  appversion:', ri.get_cmssw_version(232062)
print '  HLT key:'   , ri.get_hlt_key(232062)
print '  run key:'   , ri.get_run_key(232062)
print 'Runs 229221 and 232062'
print '  appversions:', ri.get_cmssw_versions([229221, 232062])
print '  HLT keys:'   , ri.get_hlt_keys([229221, 232062])
print '  run keys:'   , ri.get_run_keys([229221, 232062])

