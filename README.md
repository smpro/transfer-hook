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
    ## Use your custom path here
    cd daq/mwgr/transferTest

Get the list of runs to bookkeep:

    rsync -avv /opt/transferTests/runs_to_bk_*.dat .

Edit bookkeeper.py to customize the bookkeeper settings:

```python
## This should point to the output directory of the transfer hook
# _input_dir = '/store/lustre/transfer'
_input_dir = '/store/lustre/transfer_minidaq'
```

Run the bookkeeping:

    nohup ./bookkeeper.py $(cat runs_to_bk_1.dat ) >& bkeep_1.log &

Voila, that's it!