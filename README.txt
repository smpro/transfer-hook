# Update the tranfer hook on the production machine
service puppet stop
rsync -cavF /nfshome0/veverka/lib/python/smhook/smhook/ /opt/python/smhook/
service smhookd restart
service puppet start

# Rebooting srv-c2c07-16
[root@srv-C2C07-16 smhook]# service sm stop
Stopping sm: Stopping notifyworker: ..                     [FAILED]
Stopping injectworker: ..                                  [  OK  ]
Stopping copyworker: ............................................................CopyWorker did not terminate within 60 seconds, killing it!
                                                           [  OK  ]
Stopping copymanager:                                      [  OK  ]
Attempting to unmount /store/sata75a01v01
Attempting to unmount /store/lookarea
Attempting to umount /store/calibarea
                                                           [  OK  ]
# reboot # 17:40
[root@srv-C2C07-16 ~]# uptime
 17:49:37 up 6 min,  1 user,  load average: 0.15, 0.65, 0.39


# What is the srv-c2c06-17 NIC used for transfers?
The destination machine is castorcms.cern.ch.
Useful commands: ifstats, tcpdump (Andre), traceroute, ethtool ifconfig

# Dealing with large files
## Write the list of run directories for all runs starting with 238615 to the
## file dirs.txt
for r in $(ls /store/lustre/transfer/ | sed 's/run//' | awk '{if ($1 >= 238615) {print $1;}}'); do
    echo /store/lustre/transfer/run${r};
done > dirs.txt

for d in $(cat dirs.txt); do
    ls -s $d/*.dat | awk '{if ($1 > 25000000) {print $2}}';
done > above25gb.txt

# Working areas for different versions during the service deployment
transfer machine: srv-c2c07-16
area with vanilla obsolete code: /opt/transferTests
area with vanilla produciton code: /opt/python/transfer/hook
area with the service branch: /opt/transfers
area with the service_devel branch: /opt/python/smhook

# Transferring ECAL mini DAQ runs requested by Jean F.
11 runs to transfer 237033, 237041, 237043, 237048, 237049, 237052, 237053, 237059, 237078, 237083, 237086
* run 237033, lumicount 8, tot. size 55G, auto-closed OK
    thook: 2015-03-11 16:11:42-16:17:05 (6 minutes!)
* run 237041, lumicount 1, only DQM
* run 237043 - cannot find the data on Lustre
* run 237048 - cannot stat `/store/lustre/mergeMiniDAQMacro/run237048': No such file or directory
* run 237049, lumicount 3, auto-closed OK
* run 237052, lumicount 4, auto-closed OK
    thook: 2015.03.11 16:38:07-16:38:18 (11s)
* run 237053, lumicount 8, tot size 13G, auto-closed OK
    thook: 2015.03.11 16:43:09-16:43:35 (26s)
* run 237059, lumicount 5, tot size 11G, auto-closed OK
    thook 2015.03.11 16:52:59-16:53:17 (18s)
* run 237078, lumicount 9, tot size 21G, auto-closed OK
* run 237083, lumicount 7, tot size 17G, thook ca 25s, auto-closed OK
* run 237086, lumicount 9, tot size 14G, thook ca 36s, auto-closed OK



# Debugging delay of trigger scalars in WBM 
2015-03-03 18:54:06,924 __main__ INFO: Processing 3 JSON files in `/store/lustre/mergeMacro/run236705':
2015-03-03 19:06:22,685 __main__ INFO: Processing 127 JSON files in `/store/lustre/mergeMacro/run236705':
2015-03-03 20:48:54,912 __main__ INFO: Processing 16 JSON files in `/store/lustre/mergeMacro/run236713':
2015-03-03 20:51:28,500 __main__ INFO: Processing 109 JSON files in `/store/lustre/mergeMacro/run236705':
2015-03-03 22:08:52,267 __main__ INFO: Processing 16 JSON files in `/store/lustre/mergeMacro/run236721':
2015-03-03 22:08:52,402 __main__ INFO: Processing 100 JSON files in `/store/lustre/mergeMacro/run236728':
2015-03-03 22:27:33,342 __main__ INFO: Processing 30 JSON files in `/store/lustre/mergeMacro/run236736':
2015-03-03 22:27:39,897 __main__ INFO: Processing 30 JSON files in `/store/lustre/mergeMacro/run236741':
2015-03-03 22:27:49,670 __main__ INFO: Processing 758 JSON files in `/store/lustre/mergeMacro/run236743':
2015-03-03 22:39:39,021 __main__ INFO: Processing 16 JSON files in `/store/lustre/mergeMacro/run236752':
2015-03-03 22:39:39,189 __main__ INFO: Processing 380 JSON files in `/store/lustre/mergeMacro/run236755':
2015-03-03 22:46:10,371 __main__ INFO: Processing 282 JSON files in `/store/lustre/mergeMacro/run236763':
2015-03-03 22:56:32,217 __main__ INFO: Processing 758 JSON files in `/store/lustre/mergeMacro/run236764':
2015-03-03 23:05:26,955 __main__ INFO: Processing 716 JSON files in `/store/lustre/mergeMacro/run236765':
2015-03-03 23:42:16,485 __main__ INFO: Processing 16 JSON files in `/store/lustre/mergeMacro/run236766':
2015-03-03 23:42:16,553 __main__ INFO: Processing 2143 JSON files in `/store/lustre/mergeMacro/run236767':
2015-03-04 00:07:28,873 __main__ INFO: Processing 909 JSON files in `/store/lustre/mergeMacro/run236767':
2015-03-04 00:18:06,787 __main__ INFO: Processing 376 JSON files in `/store/lustre/mergeMacro/run236767':
2015-03-04 00:22:33,741 __main__ INFO: Processing 156 JSON files in `/store/lustre/mergeMacro/run236767':
2015-03-04 00:24:28,839 __main__ INFO: Processing 70 JSON files in `/store/lustre/mergeMacro/run236767':

2015-03-03 18:54:06,938

2015-03-03 18:54:06,938 __main__ INFO: Running `/opt/transferTests/injectFileIntoTransferSystem.pl --filename run236705_ls0001_streamCalibration_StorageManager.dat --path /store/lustre/transfer/run236705 --type streamer --runnumber 236705 --lumisection 1 --numevents 434 --appname CMSSW --appversion CMSSW_7_3_2_patch2 --stream Calibration --setuplabel Data --config /opt/injectworker/.db.conf --destination Global --filesize 10264660 --hltkey /cdaq/special/LS1/CRUZET/HLT_CondDBV2/V3' ...
2015-03-03 19:06:17,956 __main__ INFO: STDOUT: DB inserts completed, running Tier 0 notification script.
File sucessfully submitted for transfer.

The call above took 12 minutes!!



 For transfers:
   - everything is under /opt/transferTests on srv-c2c07-16 and mrg-c2f13-37-01. The script to be launched is watchAndInject.py with the parameter "-p /store/lustre/macroMerger", but take a look inside before actually launching it. The original idea was to have it automatically transfer everything it finds under the path, but since we'd be transferring mostly junk, the runs to be transferred are hardcoded inside. By this evening I might turn that into a parameter if I have the time, but not sure. So please just take a look at the script before launching it, it's very simple and straightforward to understand.  I've been running the actual transfers rather from the srv not to disturb the minidaq merger, but it should not really matter
   - when launching it watch on the same machine that the copyworker is actually handling the files: in /store/copyworker/Logs/CopyManager/CopyWorker_<date>.log you should see entries from the RFCP telling you it has succesfully copied the files over. If instead you only see entries of the type :
        Thu Jul 31 09:12:31 2014: LHC status: MACHINE_MODE => 'PROTON PHYSICS', BEAM_MODE => 'STABLE BEAMS', CLOCK_STABLE => 'true'
        Thu Jul 31 09:13:31 2014: LHC status: MACHINE_MODE => 'PROTON PHYSICS', BEAM_MODE => 'STABLE BEAMS', CLOCK_STABLE => 'true'
        Thu Jul 31 09:14:17 2014: Got SetID = 1
   restart the copyworker service - statistically speaking I've noticed this is particularly important in the morning, when a restart of the service changes the log file to the new dat - must be some "feature".
