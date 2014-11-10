#!/bin/bash
NBOOK=$(ls runs_to_bk_*.dat \
    | grep -v all \
    | awk -F_ '{print $4}' \
    | sed 's/.dat//' \
    | sort -n \
    | tail -n1)
RUNS_TO_BOOKKEEP="runs_to_bk_${NBOOK}.dat"
LOG_FILE="bkeep_${NBOOK}.log"

echo "Using \`$RUNS_TO_BOOKKEEP' for the run list."
echo "Logging output to \`$LOG_FILE'."

COMMAND="nohup ./bookkeeper.py \$(cat $RUNS_TO_BOOKKEEP) >& $LOG_FILE &"
echo $COMMAND
eval $COMMAND
