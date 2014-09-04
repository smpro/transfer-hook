# -*- coding: utf-8 -*-
'''
Extracts the list of streams and lumis for a given run and the number of
files for a given lumi of a given run and stream.

Jan Veverka, 3 September 2014, veverka@mit.edu
'''
import os
import json
import glob
import pprint

_input_dir = '/store/lustre/oldMergeMacro'
_run_number = 225115
_excluded_streams = ['EventDisplay', 'DQMHistograms']

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
        if stream in _excluded_streams:
            print 'Skipping stream', stream
            continue
        files_per_lumi[stream] = get_files_per_lumi(stream, last_lumi)
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
    json_map = {}
    streams = set()
    last_lumi = 0
    json_filenames.sort()
    for json in json_filenames:
        run, lumi, stream = parse_single_json_filename(json)
        streams.add(stream)
        if lumi > last_lumi:
            last_lumi = lumi
    return last_lumi, streams
## parse_json_filenames

#_______________________________________________________________________________
def parse_single_json_filename(filename):
    '''Extracts the run number, stream name and luminosity section number
    from the given filename of the json file.
    '''
    ## A JSON filename looks for example like this:
    ## run225115_ls0011_streamALCAPHISYM_StorageManager.jsn
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
def get_files_per_lumi(stream, last_lumi):
    records = []
    for lumi in range(1, last_lumi + 1):
        number_of_files = get_number_of_files(stream, lumi)
        record = dict(run             = _run_number,
                      stream          = stream,
                      lumi            = lumi,
                      number_of_files = number_of_files)
        records.append(record)
    return records
## get_files_per_lumi


#_______________________________________________________________________________
def get_number_of_files(stream, lumi):
    '''Returns the number of files for the given run, stream and luminosity
    section.  This is either 0 or 1. It is 0 if no data file is present or
    if there is no accepted events in the data file.'''
    run_dir   = os.path.join(_input_dir, 'run%d' % _run_number)
    file_name = get_json_filename(stream, lumi)
    full_path = os.path.join(run_dir, file_name)
    if not os.path.exists(full_path):
        return 0
    print 'Parsing', full_path
    with open(full_path) as source:
        meta_data = json.load(source)
    number_of_accepted_events = meta_data['data'][1]
    if number_of_accepted_events == 0:
        return 0
    else:
        return 1
## get_number_of_files


#_______________________________________________________________________________
def get_json_filename(stream, lumi):
    '''Returns the filename string for the JSON file corresponding to the
    given run, stream and lumi.'''
    ## Eample: The call get_json_filename(225115, 11, 'ALCAPHISYM') returns:
    ## 'run225115_ls0011_streamALCAPHISYM_StorageManager.jsn'
    return 'run%06d_ls%04d_stream%s_StorageManager.jsn' % (_run_number, lumi,
                                                           stream)
## get_json_filename

#_______________________________________________________________________________
def report(streams, files_per_lumi):
    pprint.pprint(streams)
    pprint.pprint(files_per_lumi)
## report


#_______________________________________________________________________________
if __name__ == '__main__':
    main()
    import user
