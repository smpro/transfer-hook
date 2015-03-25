import sys,os
import logging
import logging.config
import inspect
from configobj import ConfigObj

#mergeConfigFileName = "/opt/merger/dataFlowMerger.conf"
mergeConfigFileName = "/opt/transfers/smhookd.conf"
try:
    if os.path.isfile(mergeConfigFileName):
        config = ConfigObj(mergeConfigFileName)
    else:
        print "Configuration file not found: {0}!".format(mergeConfigFileName)
        sys.exit(1)
except IOError, e:
        print "Unable to open configuration file: {0}!".format(mergeConfigFileName)
        sys.exit(1)

loggingConfigFile = config['Misc']['logConfigFile']
logging.config.fileConfig(loggingConfigFile)

def getLogger():
    frm = inspect.stack()[1]
    mod = inspect.getfile(frm[0])
    dotPosition = mod.rfind('.')
    logging.getLogger("requests").setLevel(logging.WARNING)
    logger = logging.getLogger('Transfer.' + mod[mod.rfind('/')+1:dotPosition if dotPosition != -1 else len(mod)])
    return logger

log = getLogger()

