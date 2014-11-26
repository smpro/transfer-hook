# -*- coding: utf-8 -*-
import json
import os
import pprint
from merger.cmsDataFlowCleanUp import isCompleteRun

run_dir = '/store/lustre/mergeMacro_TEST/run220013'
#run_dir = '/store/lustre/mergeMacro/run230294'
#run_dir = '/store/lustre/mergeMacro/run230221'
suffix = 'foo'

complete = isCompleteRun(debug = 10,
                         theInputDataFolder = run_dir,
                         completeMergingThreshold = 1.0,
                         outputEndName = suffix)

eor_name = '_'.join([os.path.basename(run_dir), 'ls0000', 'MacroEoR', suffix])
with open(os.path.join(run_dir, eor_name + '.jsn')) as source:
    data = json.load(source)
isComplete = data['isComplete']

print "MacroEoR:"
pprint.pprint(data)

print "isComplete:", isComplete

if isComplete:
    print os.path.basename(run_dir), 'is complete.'
else:
    print os.path.basename(run_dir), 'is NOT complete!'
