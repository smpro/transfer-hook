# -*- coding: utf-8 -*-
import os.path
import logging
import smhook.config as config

## Default runinfo.Driver configuration
driver_cfg = config.Config(
    logging_level = logging.WARNING,
    db_config_file = os.path.join(config.DIR, '.db_runinfo_cred.py'),
    run_numbers = [229221,]
)

