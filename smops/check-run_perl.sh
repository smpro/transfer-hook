#!/bin/bash
## USAGE: sudo ./check-run.sh
SM_DB_CONF=/opt/injectworker/.db.conf
PERL5LIB=/usr/lib/perl5/vendor_perl/5.8.8 \
LD_LIBRARY_PATH=/usr/lib/oracle/10.2.0.3/client/lib \
~smpro/scripts/checkRun.pl \
    --config=$SM_DB_CONF \
    $@