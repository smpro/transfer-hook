#!/bin/bash
# $Id: t0_resend.sh,v 1.1 2009-06-16 09:40:49 loizides Exp $
#
# Starts and stops the T0 daemons and provides some cleanup between tests.
#

#
# Top directory of the T0 software
#
export T0ROOT=/nfshome0/cmsprod/TransferTest/T0

#
# T0_BASE_DIR is base directory 
#
export T0_BASE_DIR="/nfshome0/cmsprod/TransferTest"

#
# Local run directory
#
export T0_LOCAL_RUN_DIR="/nfshome0/cmsprod/node_`hostname`"

#
# Setup extra PERL environment
#
export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/ApMon_perl-2.2.6:${T0_BASE_DIR}/perl

#
# Configuration file
#
export T0_CONFIG=${T0_BASE_DIR}/Config/TransferSystem_Cessy.cfg

#
# See if we are running locally
#
if test -d /opt/copymanager; then
    export T0ROOT=/opt/copymanager
    export T0_BASE_DIR="/store/copymanager"
    export T0_LOCAL_RUN_DIR="$T0_BASE_DIR"
    export PERL5LIB=${T0ROOT}/perl_lib
    export T0_CONFIG=${T0ROOT}/TransferSystem_Cessy.cfg
fi

#
# See if we are running local test system
#
if test -d /opt/babar/copymanager; then
    export T0ROOT=/opt/babar/copymanager
    export T0_BASE_DIR="/store/babar/copymanager"
    export T0_LOCAL_RUN_DIR="$T0_BASE_DIR"
    export PERL5LIB=${T0ROOT}/perl_lib
    export T0_CONFIG=${T0ROOT}/TransferSystem_Cessy.cfg
fi

if [ ! -d $T0_BASE_DIR ]; then
    echo "$T0_BASE_DIR does not exist or is no directory"
    exit
fi

if [ ! -d $T0ROOT ]; then
    echo "$T0ROOT does not exist or is no directory"
    exit
fi

. /etc/init.d/functions
RETVAL=0
PID=""

#
# Define rules to start and stop daemons
#
start(){
    #
    # Setting up environment
    #
    mkdir -p ${T0_BASE_DIR}/Logs/Logger
    mkdir -p ${T0_BASE_DIR}/Logs/CopyManager
    mkdir -p ${T0_BASE_DIR}/Logs/TransferStatusManager

    mkdir -p ${T0_BASE_DIR}/workdir
    cd ${T0_BASE_DIR}/workdir

    dstr=`date "+%Y%m%d"`

    prog="Logger/Resender"
    echo $"Submitting $prog "
    (cat /nfshome0/cmsprod/TransferTest/operations/resend_test_zeynep.txt | /opt/copymanager/src/Logger/Resender.pl --interval 0 --config TransferSystem_Cessy.cfg ) > /store/copymanager/Logs/Logger/Resender_20160421.log 2>&1 &
    (cat /nfshome0/cmsprod/TransferTest/operations/resend_test_zeynep.txt | ${T0ROOT}/src/${prog}.pl --interval 0 --config $T0_CONFIG ) > ${T0_BASE_DIR}/Logs/${prog}_${dstr}.log 2>&1 &
    sleep 1
    echo

}

stop(){
    pidlist=""
    for pid in $(/bin/ps ax -o pid,command | grep '/usr/bin/perl -w' | grep ${T0ROOT}/src/ | grep $T0_CONFIG | sed 's/^[ \t]*//' | cut -d' ' -f 1 )
    do
      pidlist="$pidlist $pid"
    done
    if [ -n "$pidlist" ] ; then
      kill $pidlist
    fi
}

status(){
    for pid in $(/bin/ps ax -o pid,command | grep '/usr/bin/perl -w' | grep ${T0ROOT}/src/ | grep $T0_CONFIG | sed 's/^[ \t]*//' | cut -d' ' -f 1 )
    do
      echo `/bin/ps $pid | grep $pid`
    done
}

cleanup(){
    find ${T0_LOCAL_RUN_DIR}/Logs -type f -name "*.log*" -exec rm -f {} \;
    find ${T0_LOCAL_RUN_DIR}/Logs -type f -name "*.out.gz*" -exec rm -f {} \;
    find ${T0_LOCAL_RUN_DIR}/workdir/ -type f -name "*.log" -exec rm -f {} \;
}

# See how we were called.
case "$1" in
    start)
        if [ ! -z $2 ];
	  then export T0_CONFIG=$2
	fi
	echo "Using config file : $T0_CONFIG" 
	start
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo $"Usage: $0 {start|stop|status|cleanup} [config_file]"
        RETVAL=1
esac
