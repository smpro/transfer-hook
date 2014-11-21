# -*- coding: utf-8 -*-
from merger.cmsDataFlowCleanUp import isCompleteRun

run_dir = '/store/lustre/mergeMacro_TEST/run220013'

complete = isCompleteRun(debug = 10,
                         theInputDataFolder = run_dir,
                         completeMergingThreshold = 1.0,
                         outputEndName = 'foo')

print "complete:", complete
