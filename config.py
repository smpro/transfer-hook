# -*- coding: utf-8 -*-
'''
Provides support for loading Python configuration files.
'''
import os.path

#______________________________________________________________________________
class Config(object):
    '''
    Contains configuration data.
    '''
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    def load(self, pathname):
        self.__dict__.update(load(pathname).__dict__)
## Config


#______________________________________________________________________________
def load(pathname):
    '''
    Imports the given file as a module and returns it.
    '''
    config = Config()
    execfile(os.path.expanduser(pathname), {}, config.__dict__)
    return config
# load

if __name__ == '__main__':
    cfg = load('.db.int2r.stomgr_w.cfg.py')
