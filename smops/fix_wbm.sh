#!/bin/bash -x
SOURCE=/store/lustre/scratch
DESTINATION=/store/lustre/mergeMacro
RUNS_TO_FIX=$(grep IOError wai.log* | awk -F/ '{print $5}' | sort | uniq)
for RUN in $RUNS_TO_FIX; do
    echo "mv $SOURCE/$RUN/*{L1,HLT}Rates*.{jsn,jsndata} $DESTINATION/$RUN"
    mv $SOURCE/$RUN/*{L1,HLT}Rates*.{jsn,jsndata} $DESTINATION/$RUN
done
