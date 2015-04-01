# -*- coding: utf-8 -*-
'''
Provides support for loading Python and "vanilla" configuration files.
'''
import logging
import os.path
import inspect
import ConfigParser

logger = logging.getLogger(__name__)

## Directory containing this script
DIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

## Single container holding configuration for the whole application
config = None


#______________________________________________________________________________
class Config(object):
    '''
    Holds configuration data loaded from a Python config file.
    '''
    def __init__(self, **kwargs):
        self._parnames = set()
        for key, value in kwargs.items():
            self._parnames.add(key)
            setattr(self, key, value)
    def load(self, pathname):
        self.__dict__.update(load(pathname).__dict__)
        self._update()
    def _update(self):
        for key in self.__dict__.keys():
            if '_' != key[0]:
                self._parnames.add(key)
    def __str__(self):
        params = []
        for name in self._parnames:
            value = getattr(self, name)
            params.append(name + ' = ' + repr(value))
        return 'Config(%s)' % ', '.join(params)
    def __repr__(self):
        return self.__str__()
    def __eq__(self, other):
        if type(self) != type(other):
            return False
        if self._parnames != other._parnames:
            return False
        for name in self._parnames:
            if getattr(self, name) != getattr(other, name):
                return False
        return True
## Config

#______________________________________________________________________________
def init(pathname):
    '''
    Initializes the global config variable with the contents of pahtname.
    '''
    global config
    config = load(pathname)
# init


#______________________________________________________________________________
def load(pathname):
    '''
    Imports the given configuration file. The return type depends on the
    file extension of the pathname. The extension .py is interpreted as
    a python config and the config.Config object will be returned. All
    other extensions are interpreted as the ini format of the Python
    standard library's ConfigParser.
    '''
    try:
        if not os.path.isfile(pathname):
            logger.error("Configuration file not found: {0}!".format(pathname))
    except IOError, e:
        logger.error("Unable to open configuration file: {0}!".format(pathname))
        logger.exception(e)
        raise e
    root, ext = os.path.splitext(pathname)
    if ext == '.py':
        return load_py(pathname)
    else:
        return load_vanilla(pathname)
# load


#______________________________________________________________________________
def load_py(pathname):
    '''
    Imports the given python file as a Config object and returns it.
    '''
    config = Config()
    execfile(os.path.expanduser(pathname), {}, config.__dict__)
    config._update()
    return config
# load_py


#______________________________________________________________________________
def load_vanilla(pathname):
    '''
    Parses the given config file and returns the resulting ConfigParser object.
    '''
    config = ConfigParser.ConfigParser()
    config.read(pathname)
    return config
# load_vanilla


#______________________________________________________________________________
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    ## Load a python config
    cfg = load(os.path.join(DIR, '.db_int2r_cred.py'))    
    ## Load a vanilla config
    init('smhookd.conf')
    print "config.get('Input', 'path'):", config.get('Input', 'path')
    print "config.get('Streams', 'streams_to_dqm'):", config.get('Streams', 'streams_to_dqm')
    streams = map(str.strip, config.get('Streams', 'streams_to_dqm').split(','))
