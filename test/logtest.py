#!/bin/env python
# -*- coding: utf-8 -*-
'''
This file helps to understand the behaviour of the logging and its interaction
with the merger code.

Jan Veverka, 25. November 2014, veverka@mit.edu

USAGE:
    ./logtest.py

Example output:
*********** BEFORE CONFIG ***********
logger: <logging.Logger instance at 0x7f3627003170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1ade440>
logger: <logging.RootLogger instance at 0x1ade440>
    name: root
    level: WARNING
    handlers: []

*********** AFTER CONFIG ***********
logger: <logging.Logger instance at 0x7f3627003170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1ade440>
logger: <logging.RootLogger instance at 0x1ade440>
    name: root
    level: DEBUG
    handlers: [<logging.FileHandler instance at 0x7f36270033b0>]
     <logging.FileHandler instance at 0x7f36270033b0>
        baseFilename: /cmsnfshome0/nfshome0/veverka/lib/python/transfer/hook/test/test.log
        formatter: <logging.Formatter instance at 0x1ade7a0>
        formatter._fmt: %(levelname)s in %(name)s: %(message)s

*********** AFTER IMPORT ***********
logger: <logging.Logger instance at 0x7f3627003170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1ade440>
logger: <logging.RootLogger instance at 0x1ade440>
    name: root
    level: DEBUG
    handlers: [<logging.StreamHandler instance at 0x1b92368>]
     <logging.StreamHandler instance at 0x1b92368>
        formatter: <logging.Formatter instance at 0x1b92878>
        formatter._fmt: %(message)s

*********** AFTER RECONFIG ***********
logger: <logging.Logger instance at 0x7f3627003170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1ade440>
logger: <logging.RootLogger instance at 0x1ade440>
    name: root
    level: DEBUG
    handlers: [<logging.StreamHandler instance at 0x1b92368>]
     <logging.StreamHandler instance at 0x1b92368>
        formatter: <logging.Formatter instance at 0x1b92878>
        formatter._fmt: %(message)s

*********** AFTER HACK ***********
logger: <logging.Logger instance at 0x7f3627003170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1ade440>
logger: <logging.RootLogger instance at 0x1ade440>
    name: root
    level: DEBUG
    handlers: []

*********** AFTER HACK + RECONFIG ***********
logger: <logging.Logger instance at 0x7f3627003170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1ade440>
logger: <logging.RootLogger instance at 0x1ade440>
    name: root
    level: DEBUG
    handlers: [<logging.FileHandler instance at 0x1b96320>]
     <logging.FileHandler instance at 0x1b96320>
        baseFilename: /cmsnfshome0/nfshome0/veverka/lib/python/transfer/hook/test/test.log
        formatter: <logging.Formatter instance at 0x1b96488>
        formatter._fmt: %(levelname)s in %(name)s: %(message)s
'''
import logging
logger = logging.getLogger(__name__)

def main():
    inspect_logging('*********** BEFORE CONFIG ***********')
    config()
    inspect_logging('*********** AFTER CONFIG ***********')
    import merger.cmsDataFlowCleanUp
    inspect_logging('*********** AFTER IMPORT ***********')
    config()
    inspect_logging('*********** AFTER RECONFIG ***********')
    del logger.root.handlers[0]
    inspect_logging('*********** AFTER HACK ***********')
    config()
    inspect_logging('*********** AFTER HACK + RECONFIG ***********')

def inspect_logging(msg='***'):
    print '\n' + msg
    inspect(logger)

def config():
    logging.basicConfig(level = logging.DEBUG,
                        filename='test.log',
                        format='%(levelname)s in %(name)s: %(message)s',)

def inspect(logger):
    print 'logger:', logger
    print '    name:', logger.name
    print '    level:', logging.getLevelName(logger.level)
    print '    handlers:', logger.handlers
    for handler in logger.handlers:
        print '    ', handler
        if hasattr(handler, 'baseFilename'):
            print '        baseFilename:', handler.baseFilename
        print '        formatter:', handler.formatter
        print '        formatter._fmt:', handler.formatter._fmt
    if logger.parent:
        print '    parent:', logger.parent
        inspect(logger.parent)

if __name__ == '__main__':
    main()
