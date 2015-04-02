# -*- coding: utf-8 -*-
'''
Provides classes that facilitate parsing of CMS DAQ2 meta-file filenames and
contents. For example,
    
    >>> myjson = Filename('/store/lustre/mergeMacro/run229878/run229878_ls0021_streamA_StorageManager.jsn')
    >>> myjson.run 
    229878
    >>> myjson.ls
    21
    >>> myjson.stream 
    'A'
    >>> myjson.type == Type.MacroMerger
    True
    
The examples of supported JSON file types include:

    * run229878_ls0021_streamA_StorageManager.jsn
    * run229878_ls0019_streamL1Rates_mrg-c2f13-35-01.jsn
    * run229878_ls0000_MiniEoR_bu-c2e18-09-01.jsn
        
USAGE:
    python metafile.py ## run unit tests
    python metafile.py -v ## run unit tests with verbose output
'''

__author__     = 'Jan Veverka'
__copyright__  = 'Unknown'
__credits__    = []
__licence__    = 'Unknown'
__version__    = '0.1.1'
__maintainer__ = 'Jan Veverka'
__email__      = 'veverka@mit.edu'
__status__     = 'Development'


import os
import json
import enum

## Enumerates different types of JSON meta files.
Type = enum.enum('MacroMerger', 'MiniEoR', 'Unknown')

#______________________________________________________________________________
class Filename(object):
    '''
    Takes a filename of a meta-data file, parses it and stores the results in
    its attributes.
    
    The given path is required to be of the form run<N>_ls<M>_*_*.jsn
    with <N> and <M> denoting integers, eventually padded with zeroes:
        
        >>> Filename('foo')
        Traceback (most recent call last):
            ...
        ValueError: Bad filename `foo', expect `.jsn' extension!

        >>> Filename('foo.jsn')
        Traceback (most recent call last):
            ...
        ValueError: Bad filename `foo.jsn', expect `*_*_*_*.jsn' form!
        
        >>> Filename('f_o_o_bar.jsn')
        Traceback (most recent call last):
            ...
        ValueError: Bad filename `f_o_o_bar.jsn', expect `run<N>_*_*_*.jsn' form!
        
    '''
    def __init__(self, path):
        self.path      = path
        self.dirname   = os.path.dirname(path)
        self.basename  = os.path.basename(path)
        self._parse_basename()

    def _parse_basename(self):
        root, ext = os.path.splitext(self.basename)
        if not ext == '.jsn':
            self._raise_bad_filename("expect `.jsn' extension")
        tokens = root.split('_')
        if not len(tokens) == 4:
            self._raise_bad_filename("expect `*_*_*_*.jsn' form")
        run_token, lumi_token, type_token, hostname_token = tokens
        self._parse_runnumber(run_token     )
        self._parse_lumi     (lumi_token    )
        self._parse_type     (type_token    )
        self._parse_hostname (hostname_token)

    def _raise_bad_filename(self, msg=''):
        if msg:
            msg = ', ' + msg
        raise ValueError, "Bad filename `%s'%s!" % (self.basename, msg)

    def _parse_runnumber(self, token):
        try:
            if 'run' not in token:
                raise ValueError
            self.run = int(token.replace('run', ''))
        except ValueError:
            self._raise_bad_filename("expect `run<N>_*_*_*.jsn' form")

    def _parse_lumi(self, token):
        try:
            if 'ls' not in token:
                raise ValueError
            self.ls = int(token.replace('ls', ''))
        except ValueError:
            self._raise_bad_filename("expect `*_ls<M>_*_*.jsn' form")
            
    def _parse_type(self, token):
        if 'stream' in token:
            self.type = Type.MacroMerger
            self._parse_stream(token)
        elif 'MiniEoR' in token:
            self.type = Type.MiniEoR
        else:
            self.type = Type.Unknown
            
    def _parse_stream(self, token):
        self.stream = token.replace('stream', '')
        
    def _parse_hostname(self, token):
        if self.type == Type.MiniEoR or (self.type == Type.MacroMerger and 
                                         token     != 'StorageManager'):
            self.hostname = token
## Filename


#______________________________________________________________________________
class File(Filename):
    def __init__(self, path):
        Filename.__init__(self, path)
        self._load()

    def _load(self):
        with open(self.path) as source:
            self.data = json.load(source)
## File


#______________________________________________________________________________
class MiniEoRFile(File):
    def __init__(self, path):
        File.__init__(self, path)
        if self.type != Type.MiniEoR or self.ls != 0:
            self._raise_bad_filename(
                "expect `run<N>_ls0000_MiniEoR_<bu>.jsn' form!"
                )
        self._parse_data()
        
    def _parse_data(self):
        for key in 'numberBoLSFiles eventsInputFU eventsTotalEoR'.split():
            value = self.data[key]
            setattr(self, key, value)
            
    def is_run_complete(self):
        return (self.numberBoLSFiles == 0 and
                self.eventsInputFU   == self.eventsTotalEoR)
## MiniEoRFile

#______________________________________________________________________________
class MacroMergerFile(File):
    def __init__(self, path):
        File.__init__(self, path)
        if self.type != Type.MacroMerger:
            self._raise_bad_filename(
                "expect `run<N>_ls<M>_stream<A>_<suffix>.jsn' form!"
                )
        self._parse_data()

    def _parse_data(self):
        # https://twiki.cern.ch/twiki/bin/view/CMS/FFFMetafileFormats#Jsn_Per_output_file
        self.processed         = self.data['data'][0]
        self.accepted          = self.data['data'][1]
        self.return_code_mask  = self.data['data'][2]
        self.file_name         = self.data['data'][3]
        self.file_size         = self.data['data'][4]
        self.file_adler32      = self.data['data'][5]
        self.n_files           = self.data['data'][6]
        self.n_total_processed = self.data['data'][7]
## MacroMergerFile

#______________________________________________________________________________
if __name__ == '__main__':
    import doctest
    doctest.testmod()
