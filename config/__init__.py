# -*- coding: utf-8 -*-
'''
Provides support for loading Python configuration files.
'''
import os.path
import inspect

## Directory containing this script
DIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

#______________________________________________________________________________
class Config(object):
    '''
    Contains configuration data.
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
def load(pathname):
    '''
    Imports the given file as a Config object and returns it.
    '''
    config = Config()
    execfile(os.path.expanduser(pathname), {}, config.__dict__)
    config._update()
    return config
# load

if __name__ == '__main__':
    cfg = load(os.path.join(DIR, '.db_int2r_cred.py'))
