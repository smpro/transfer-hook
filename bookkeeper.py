# -*- coding: utf-8 -*-
'''
Extracts the list of streams and lumis for a given run and the number of
files for a given lumi of a given run and stream.

Jan Veverka, 3 September 2014, veverka@mit.edu
'''
import os
import glob
import pprint

_input_dir = '/store/lustre/oldMergeMacro'
_run_number = 225115
_exclude_stream = ['EventDisplay', 'DQMHistograms']

#_______________________________________________________________________________
def main():
    run_dir            = os.path.join(_input_dir, 'run%d' % _run_number)
    json_filenames     = get_json_filenames(run_dir)
    last_lumi, streams = parse_json_filenames(json_filenames)
    ## Dictionary of lists giving the number of files for given lumi and stream.
    ## Stream is given by the dictionary key. The dictionary value is a list
    ## of the file counts with one entry per each lumi section. The lumi section
    ## is given by the index of the list item increased by 1:
    ## s -> [files_per_lumi_1, files_per_lumi_2, ..., files_per_lumi_N]
    ## Here, files_per_lumi_i is the number of files in the lumi i of
    ## the stream S. The index of this element is j = i - 1, so that i = j + 1.
    ## N is the total number of lumis, which is equal to the number of the
    ## last lumi stored in the last_lumi variable
    files_per_lumi = {}
    for stream in streams:
        files_per_lumi[stream] = get_files_per_lumi(json_filenames, stream,
                                                    last_lumi)
    report(streams, files_per_lumi)
## main


#_______________________________________________________________________________
def get_json_filenames(run_dir):
    mask = '*.jsn'
    pathname = os.path.join(run_dir, mask)
    json_filenames = glob.glob(pathname)
    json_filenames = [os.path.basename(f) for f in json_filenames]
    return json_filenames
## get_json_filenames


#_______________________________________________________________________________
def parse_json_filenames(json_filenames):
    streams = set()
    last_lumi = 0
    json_filenames.sort()
    for json in json_filenames:
        run, lumi, stream = parse_single_json_filename(json)
        streams.add(stream)
        if lumi > last_lumi:
            last_lumi = lumi
    pprint.pprint(streams)
    print 'Last lumi:', last_lumi
    return last_lumi, streams
## parse_json_filenames

#_______________________________________________________________________________
def parse_single_json_filename(filename):
    '''Extracts the run number, stream name and luminosity section number
    from the given filename of the json file.  An example json filename is:
    run225115_ls0011_streamALCAPHISYM_StorageManager.jsn
    '''
    root, extention = os.path.splitext(filename)
    tokens = root.split('_')
    skip_message = "INFO: Skipping `%s' ..." % filename
    if len(tokens) != 4:
        print skip_message
        return None
    run_token, ls_token, stream_token, sm_token = tokens
    if (run_token   [:len('run')   ] != 'run'         or
        ls_token    [:len('ls')    ] != 'ls'          or
        stream_token[:len('stream')] != 'stream'      or
        sm_token                     != 'StorageManager'):
        print skip_message
        return None

    run    = int(run_token   [len('run')   :])
    lumi   = int(ls_token    [len('ls')    :])
    stream =     stream_token[len('stream'):]
    return run, lumi, stream
## parse_single_json_filename


#_______________________________________________________________________________
def get_files_per_lumi(json_filenames, stream, last_lumi):
    files_per_lumi = []
    return files_per_lumi
## get_files_per_lumi


#_______________________________________________________________________________
def report(streams, files_per_lumi):
    for stream in streams:
        for index, files in enumerate(files_per_lumi[stream]):
            lumi = index + 1
            print 'stream: %s, lumi: %d, files: %d' % (stream, lumi, files)
## report


#_______________________________________________________________________________
if __name__ == '__main__':
    main()
    import user
