#!/bin/bash
##
## Lists runs in the folder $INPUT_DIR that are in the range between
## $FIRST_RUN and $LAST_RUN given by the watchAndInject.py script.
## The list is order by the ascending run number.
##
## Jan Veverka, 27 Oct 2014, veverka@mit.edu
##
## USAGE:
##     ./list-runs-numbers.sh
## Example output:
##      228148
##      228452
##      228455

INPUT_DIR=/store/lustre/mergeMacro
#FIRST_RUN=$(grep '_run_number_min =' watchAndInject.py | awk '{print $3}')
FIRST_RUN=228892
LAST_RUN=$(grep '_run_number_max =' watchAndInject.py | awk '{print $3}')

ls $INPUT_DIR \
    | grep -E '^run[[:digit:]]{6,6}$' \
    | sed 's/run//' \
    | awk "{if ($FIRST_RUN <= \$1 && \$1 <= $LAST_RUN) {print \$1}}" \
    | sort -n
