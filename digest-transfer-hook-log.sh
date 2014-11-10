#!/bin/bash
## Counts the numnber of files injected
## in the transfer for each run.
## The log file(s) to digest can be specified in
## 3 different ways, in the order of increasing 
## priorities:
##    * Default is hard-coded below
##    * The variable XFER_HOOK_LOG if it exists
##    * Parameters on the command line
## USAGE:
##    ./digest-transfer-hook-log.sh

## The default logs
LOG=$(ls -rt /opt/transferTests/transfer_*.log | tail -1)

## Take the logs from the enviromnet if available
if [[ ! -z "$XFER_HOOK_LOG" ]]; then
    LOG=$XFER_HOOK_LOG
fi

## Take the logs from the command line if available
if [[ ! -z "$1" ]]; then
    LOG="$@"
fi

COMMAND="echo 'Transferred files'; echo 'Count Run';\
  grep injectFileIntoTransferSystem.pl $LOG |\
  grep -v -- --check |\
  awk '{print \$9}' |\
  grep -E '[[:digit:]{6}]' |\
  sort |\
  uniq -c"

echo "$COMMAND"
eval $COMMAND

