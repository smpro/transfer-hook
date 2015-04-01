#!/bin/bash
for RUN in "$@"; do 
    EOR=$(ls /store/lustre/transfer/run${RUN}/*MacroEoR*.jsn); 
    echo "Run $RUN failed to close automatically"
    echo "Mismatch between eventsTotalRun, eventsInputBUs and eventsStreamInput:"
    echo
    echo "{code:title=$EOR}"
    cat $EOR
    echo
    echo "{code}"
    echo
done
