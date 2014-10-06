#!/bin/bash
##
## USAGE:
##   $ ./digest-copyworker-log.sh &
##
PROGRESS_LOG=progress.log
SECONDS_TO_SLEEP=10
echo "Appending output to $PROGRESS_LOG every $SECONDS_TO_SLEEP seconds."
while true; do
    COPYWORKER_LOG=$(ls -rt /store/copyworker/Logs/CopyManager/CopyWorker_*.log | tail -1)
    HEADER="$(date): $(basename $COPYWORKER_LOG) has"
    SCOUNT="$(grep succeeded $COPYWORKER_LOG | wc -l)"
    ECOUNT="$(grep uninitialized $COPYWORKER_LOG | wc -l)"
    MESSAGE="$HEADER $SCOUNT succeeded transfers and $ECOUNT 'uninitialized' errors."
    echo $MESSAGE >> $PROGRESS_LOG
    sleep $SECONDS_TO_SLEEP
done
