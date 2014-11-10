#!/bin/bash
## The files `runs_to_bk_{1...${NBOOK}}.dat` contain the lists of run numbers
## that have already been bookkept, with $N giving the last one.  We do *not*
## want to repeat the bookkeeping for these.  Therefore, we will create a new
## file `runs_to_bk_${M}.dat`, with $M = $NBOOK + 1, containing only the runs,
## for which we need to run the bookkeeping.

NBOOK=$(ls runs_to_bk_*.dat \
    | grep -v all \
    | awk -F_ '{print $4}' \
    | sed 's/.dat//' \
    | sort -n \
    | tail -n1)
((NBOOK = $NBOOK + 1))
cat runs_to_bk_*.dat | sort -n | uniq -u > runs_to_bk_${NBOOK}.dat \
    && echo "Created \`runs_to_bk_${NBOOK}.dat'"
