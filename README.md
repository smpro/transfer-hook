# Transfer Hook

https://github.com/smpro/transfer-hook

Hooks the merger output to the transfer system

## Github Recipes

Initialize tunnel to github, see
https://twiki.cern.ch/twiki/bin/viewauth/CMS/ClusterUsersGuide#How_to_get_access_to_github_for

    ssh cmsusr.cms -f -N -D 10800

Checkout the code.

    cd ~/daq/mwgr
    git clone git@github.com:smpro/transfer-hook.git

## Transfer Recipes

Setup the transfers:

    ssh srv-c2c07-16
    sudo su -
    service sm status
    cd /opt/transferTests

Sync the code with a repository at P5 that has access to github:

    rsync -avvF ~veverka/daq/mwgr/transferTest/ .

Start logging the progress by listing the number of transferred files and
errors.  This is done every 10 seconds by default:

    nohup ./digest-copyworker-log.sh >& progress.log &

Edit `watchAndInject.py` to customize the settings like:

  * the range of run numbers to consider,
  * streams to exclude,
  * output path (differs for mini DAQ and Global data taking)
  * etc.

Then, launch the transfers:

    nohup ./watchAndInject.py -p /store/lustre/mergeMacro >& transfer.log &

Inspect the progress of the transfer hook and of the transfer system:

    tail transfer.log
    tail progress.log

Get a summary of the transfer hook log file:

    ./digest-transfer-hook-log.sh transfer.log

The output should look something like this:

```bash
echo 'Transferred files'; echo 'Count Run';  grep injectFileIntoTransferSystem.pl transfer_minidaq.log |  grep -v -- --check |  awk '{print $9}' |  grep -E '[[:digit:]{6}]' |  sort |  uniq -c
Transferred files
Count Run
     68 227914
    102 227921
    102 227927
    120 227931
     10 227944
     82 227951
   4518 227957
```

## Bookkeeping Recipes

Setup the environment:

    ssh srv-c2c07-16
    sudo su -
    cd /opt/transferTests

Get the list of all the runs to bookkeep:

    ./list-runs.sh > new_runs_to_bk.dat
    cat new_runs_to_bk.dat runs_to_bk_*.dat | sort | uniq > runs_to_bk_all.dat

Edit `runs_to_bk_all.dat` and remove all runs that may still be ongoing or
being transferred. Use your judgement.

The output of the scripts `list-runs-and-times.sh` and
`digest-transfer-hook-log.sh` may be helpful. You can see the list of runs
with times of the last changes to the run folder like this:

    ./list-runs-and-times.sh

You can see the number of files that have been submitted to the transfer
broken down by the run number like this:

    ./digest-transfer-hook-log.sh transfer.log

Here, `transfer.log` should be the name of the most recent log file containing
the output of `watchAndInject.py`.

The files `runs_to_bk_{1...${NBOOK}}.dat` contain the lists of run numbers that
have already been bookkept, with $N giving the last one.  We do *not* want to
repeat the bookkeeping for these.  Therefore, we will create a new file
`runs_to_bk_${M}.dat`, with $M = $NBOOK + 1, containing only the runs, for which
we need to run the bookkeeping.

    ./create-list-of-runs-to-bookkeep.sh

The output should look like this:

```bash
Created `runs_to_bk_2.dat'
```

Edit bookkeeper.py to customize the bookkeeper settings:

```python
## This should point to the output directory of the transfer hook
# _input_dir = '/store/lustre/transfer'
_input_dir = '/store/lustre/transfer_minidaq'
```

Launch the bookkeeping:

    . launch-bookkeeping.sh

The output should look like this:
```bash
Using `runs_to_bk_2.dat' for the run list.
Logging output to `bkeep_2.log'.
nohup ./bookkeeper.py $(cat runs_to_bk_2.dat) >& bkeep_2.log &
```

#RPM Recipe

This recipe will be automatized by a bash wrapper but for the time being
the steps are documented here.

You need to start with the following command:
    rpmdev-setuptree

This creates all the relevant directories/sub directories that needs to
exist to be able to build an RPM. You then go into the 
SPEC folder, I run the following command:

    rpmdev-newspec	

This will create you a template spec file, you can rename it/edit it. The 
spec file used in this RPM is in git. You need to also copy a .tgz of the 
source code under the SOURCE directory. Lastly to build the RPM execute 
the following command:
    
    rpmbuild -ba smhook.spec

Voila, that's it!
