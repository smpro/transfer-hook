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

The transfers are using multiple machines the mapping is:
    
    srv-c2c07-16 --> Minidaq (minidaq configurations to be used)
    mrg-c2f12-25 --> CDaq (cdaq configuration to be used)
    mrg-c2f12-20 --> Testing (test configuration to be used)

Transfer system has multiple components running on each machine.
To check the the status of the system do:

   service copyworker status
   service injectworker status
   service notifyworker status
   service smhookd status
   service smeord status

Inspect the progress of the transfer hook and of the transfer system:

    tail /var/log/smhook.log
    tail /var/log/smeor.log

## Bookkeeping Recipes

Setup the environment:

    ssh mrg-c2f12-25-01
    sudo su -
    cd /opt/python/smhook

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

Edit bookkeeper.py to customize the bookkeeper settings (this is done automatically
through the config file through eor in the production):

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

To build an RPM, commit the changes you have done on git branch devel.
The wrapper to be used is under smhook directory. All you have to do is:

    ./mkrpm.sh

This wrapper relies on the TEMP.spec file located in the same directory.
When you run this command, you will end up with an rpm under: ~/SMHOOK_RPM/xxxx

Voila, that's it!
