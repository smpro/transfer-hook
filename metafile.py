# -*- coding: utf-8 -*-
'''
Provides classes that facilitate parsing of CMS DAQ2 meta-file filenames and
contents.
'''

__author__     = 'Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = []
__licence__    = 'Unknonw'
__version__    = '0.1.1'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'


#_______________________________________________________________________________
class FileType(object):
    '''
    Enumerates different types of JSON meta files.
    '''
    MiniMerger = 1
    MacroMerger = 2
    MiniEoR = 3
## FileType


#_______________________________________________________________________________
class FileName(object):
    def __init__(self, path):
        self.path = path
        self.run = 0
        self.stream = ''
        self.lumi = 0
        self.file_type = FileType.MiniEoR
## Filename


#_______________________________________________________________________________
class File(FileName):
    def __init__(self, path):
        FileName.__init__(self, path)
        pass
## File
