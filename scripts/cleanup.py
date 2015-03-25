#!/bin/env python
# -*- coding: utf-8 -*-

import glob
import logging
import os
import pprint
import subprocess

import transfer.hook.eor as eor
from transfer.hook.metafile import Filename

logger = logging.getLogger(__name__)

_STATES = map('FILES_'.__add__, '''CREATED INJECTED TRANS_NEW TRANS_COPIED
                                   TRANS_CHECKED TRANS_INSERTED
                                   TRANS_REPACKED DELETED'''.split())

#______________________________________________________________________________
def main():
    setup()
    logger.info('Started ...')
    cleanup_runs_under_path('/store/lustre/transfer')
    logger.info('Exiting with great success!')

#______________________________________________________________________________
def setup():
    configure_logging()
    make_sure_perl_phrasebook_works()

#______________________________________________________________________________
def configure_logging():
    logging.basicConfig(
        format = r'%(asctime)s %(name)s %(levelname)s: %(message)s',
        level = logging.DEBUG
    )


#______________________________________________________________________________
def make_sure_perl_phrasebook_works():
    varname = 'PERL5LIB'
    required_path = '/usr/lib/perl5/vendor_perl/5.8.8'
    if varname in os.environ:
        if required_path in os.environ[varname].split(':'):
            logger.debug(required_path + ' already part of ' + varname)
        else:
            logger.info(
                'Appending %s to the environment variable %s ...' % (
                    required_path, varname
                )
            )
            os.environ[varname] += ':' + required_path
    else:
        logger.info(
            'Setting environment variable %s=%s ...' % (
                varname, required_path
            )
        )
        os.environ[varname] = required_path

#______________________________________________________________________________
def cleanup_runs_under_path(path):
    logging.info("Cleaning up `%s' ..." % path)
    if not os.path.exists(path):
        logging.error("Path `%s' doesn't exist!" % path)
        return 1
    rundirs = glob.glob(os.path.join(path, 'run*'))
    runs = map(eor.Run, rundirs)[:2]
    logging.info("Inspecting runs " + repr([r.number for r in runs]))
    script_path = '/nfshome0/smpro/scripts/checkRun.pl'
    for run in runs:
        shell_args = [script_path, '--full', '--runnumber=%d' % run.number]
        out, err = log_and_exec(shell_args)
        state_map = parse_file_states(out)
        logger.debug('state_map: ' + pprint.pformat(state_map))
        state_to_delete = 'FILES_TRANS_INSERTED'
        if state_to_delete in state_map:
            files_to_delete = map(get_full_path, 
                                  state_map[state_to_delete]['files'])
            total_size = state_map[state_to_delete]['size']
            logger.info('To delete: ' + pprint.pformat(files_to_delete))
            logger.info(
                '%d files with total size of %d' % (
                    len(files_to_delete), total_size
                )
            )
    logging.info("Finished cleaning up `%s' ..." % path)


#_______________________________________________________________________________
def log_and_exec(args, print_output=True):
    ## Make sure all arguments are strings; cast integers.
    args = map(str, args)
    logger.info("Running `%s' ..." % ' '.join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if print_output:
        if out:
            logger.info('STDOUT: ' + str(out))
        if err:
            logger.info('STDERR: ' + str(err))
    return out, err
## log_and_exec()


#_______________________________________________________________________________
def parse_file_states(text):
    state_map = {}
    default_value = {'size': 0, 'files': []}
    current_state = None
    for line in text.split('\n'):
        found_state = False
        for state in _STATES:
            if state in line:
                current_state = state
                logger.debug('Parsing files in state %s ...' % state)
                found_state = True
                break
        if found_state is True:
            continue
        if current_state is None:
            continue
        try:
            filename, host, size_string = line.split()
        except Exception as err:
            logger.warning("Failed to parse line: `%s'!" % line)
            logger.exception(err)
            continue
        state_data = state_map.setdefault(current_state, default_value)
        state_data['files'].append(filename)
        state_data['size'] += int(size_string)
    return state_map

#_______________________________________________________________________________
def get_full_path(filename):
    run_number = int(filename.split('_')[0].replace('run', ''))
    return os.path.join(
        '/store/lustre/transfer', 'run%d' % run_number, filename
    )

#______________________________________________________________________________
if __name__ == '__main__':
    main()
