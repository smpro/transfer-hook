#!/bin/bash
##
## Lists runs and times corresponding to folders in $INPUT_DIR that are in
## the range between $FIRST_RUN and $LAST_RUN given by the watchAndInject.py
## script.
## The list is order by the ascending run number.
##
## Jan Veverka, 27 Oct 2014, veverka@mit.edu
##
## USAGE:
##     ./list-runs-and-times.sh
##
## Example output:
##     Listing runs between 228147 and 300000 ...
##     Run      Taken on
##     ---------------------
##     228452   Oct 27 11:51
##     228455   Oct 27 12:21
##     228463   Oct 27 13:22

INPUT_DIR=/store/lustre/mergeMacro
#FIRST_RUN=$(grep '_run_number_min =' watchAndInject.py | awk '{print $3}')
FIRST_RUN=228912
LAST_RUN=$(grep '_run_number_max =' watchAndInject.py | awk '{print $3}')

echo "Listing runs between $FIRST_RUN and $LAST_RUN ..."
echo "Run      Taken on"
echo "---------------------"

## Example line of the ls output:
##      drwxrwxrwx 4 root root   20480 Oct 27 15:33 run228489
## Numbers of the fields in awk:
##      $1         $2 $3  $4     $5    $6  $7 $8    $9
ls -l /store/lustre/mergeMacro \
    | grep -E 'run[[:digit:]]{6,6}$' \
    | sed 's/run//' \
    | awk "{if ($FIRST_RUN <= \$9 && \$9 <= $LAST_RUN)
             {print \$9, \" \", \$6, \$7, \$8}}" \
    | sort -n
