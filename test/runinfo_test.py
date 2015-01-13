# -*- coding: utf-8 -*-
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

