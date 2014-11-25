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
logger: <logging.Logger instance at 0x7f39b5264170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1b7f440>
logger: <logging.RootLogger instance at 0x1b7f440>
    name: root
    level: WARNING
    handlers: []

*********** AFTER CONFIG ***********
logger: <logging.Logger instance at 0x7f39b5264170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1b7f440>
logger: <logging.RootLogger instance at 0x1b7f440>
    name: root
    level: INFO
    handlers: [<logging.FileHandler instance at 0x7f39b52643b0>]
     <logging.FileHandler instance at 0x7f39b52643b0>
        baseFilename: /cmsnfshome0/nfshome0/veverka/lib/python/transfer/hook/test/test.log
        formatter: <logging.Formatter instance at 0x1b7f7a0>
        formatter._fmt: %(levelname)s in %(name)s: %(message)s

*********** AFTER IMPORT ***********
logger: <logging.Logger instance at 0x7f39b5264170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1b7f440>
logger: <logging.RootLogger instance at 0x1b7f440>
    name: root
    level: DEBUG
    handlers: [<logging.StreamHandler instance at 0x1c34368>]
     <logging.StreamHandler instance at 0x1c34368>
        formatter: <logging.Formatter instance at 0x1c34878>
        formatter._fmt: %(message)s

*********** AFTER RECONFIG ***********
logger: <logging.Logger instance at 0x7f39b5264170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1b7f440>
logger: <logging.RootLogger instance at 0x1b7f440>
    name: root
    level: DEBUG
    handlers: [<logging.StreamHandler instance at 0x1c34368>]
     <logging.StreamHandler instance at 0x1c34368>
        formatter: <logging.Formatter instance at 0x1c34878>
        formatter._fmt: %(message)s

*********** AFTER HACK ***********
logger: <logging.Logger instance at 0x7f39b5264170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1b7f440>
logger: <logging.RootLogger instance at 0x1b7f440>
    name: root
    level: DEBUG
    handlers: []

*********** AFTER HACK + RECONFIG ***********
logger: <logging.Logger instance at 0x7f39b5264170>
    name: __main__
    level: NOTSET
    handlers: []
    parent: <logging.RootLogger instance at 0x1b7f440>
logger: <logging.RootLogger instance at 0x1b7f440>
    name: root
    level: INFO
    handlers: [<logging.FileHandler instance at 0x1c38320>]
     <logging.FileHandler instance at 0x1c38320>
        baseFilename: /cmsnfshome0/nfshome0/veverka/lib/python/transfer/hook/test/test.log
        formatter: <logging.Formatter instance at 0x1c38488>
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
    hack()
    inspect_logging('*********** AFTER HACK ***********')
    config()
    inspect_logging('*********** AFTER HACK + RECONFIG ***********')

def inspect_logging(msg='***'):
    print '\n' + msg
    inspect(logger)

def config():
    logging.basicConfig(level = logging.INFO,
                        filename='test.log',
                        format='%(levelname)s in %(name)s: %(message)s',)

def hack():
    logger.root.handlers = []
    logger.disabled = 0

def inspect(logger):
    print 'logger:', logger
    print '    name:    ', logger.name
    print '    disabled:', logger.disabled
    print '    level:   ', logging.getLevelName(logger.level)
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
