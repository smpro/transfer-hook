#!/bin/env python                                                                                                                                                                                                                           

import os,sys,socket,time,shutil,json,datetime,logging
import requests
from datetime import datetime, timedelta, date

logger = logging.getLogger(__name__)

#______________________________________________________________________________
def elasticMonitor(monitorData, esServerUrl, esIndexName, documentId, maxConnectionAttempts):
   # here the merge action is monitored by inserting a record into Elastic Search database                                                                                                    
   connectionAttempts=0 #initialize                                                                                                                                                          
   # make dictionary to be JSON-ified and inserted into the Elastic Search DB as a document
   keys   = ["processed","accepted","errorEvents","fname","size","eolField1","eolField2","fm_date","runNumber","ls","stream","type","status"]
   values = [int(f) if str(f).isdigit() else str(f) for f in monitorData]
   transferMonitorDict=dict(zip(keys,values))
   transferMonitorDict['startTime']=transferMonitorDict['fm_date']
   transferMonitorDict['host']=os.uname()[1]
   while True:
       try:
           documentType='transfer'
           logger.info("About to try to insert into ES with the following info:")
           logger.info('Server: "' + esServerUrl+'/'+esIndexName+'/'+documentType+'/' + '"')
           logger.info("Data: '"+json.dumps(transferMonitorDict)+"'")
           monitorResponse=requests.put(esServerUrl+'/'+esIndexName+'/'+documentType+'/'+documentId,data=json.dumps(transferMonitorDict),timeout=1)
           if monitorResponse.status_code not in [200,201]:
               logger.error("elasticsearch replied with error code {0} and response: {1}".format(monitorResponse.status_code,monitorResponse.text))
           logger.debug("{0}: Merger monitor produced response: {1}".format(datetime.now().strftime("%H:%M:%S"), monitorResponse.text))
           break
       except (requests.exceptions.ConnectionError,requests.exceptions.Timeout) as e:
           logger.error('elasticMonitor threw connection error: HTTP ' + monitorResponse.status_code)
           logger.error(monitorResponse.raise_for_status())
           if connectionAttempts > maxConnectionAttempts:
               logger.error('connection error: elasticMonitor failed to record '+documentType+' after '+ str(maxConnectionAttempts)+'attempts')
               break
           else:
               connectionAttempts+=1
               time.sleep(0.1)
           continue

#______________________________________________________________________________
def elasticMonitorUpdate(monitorData, esServerUrl, esIndexName, documentId, maxConnectionAttempts):
   # here the merge action is monitored by updating a record in Elastic Search database                                                                                                    
   connectionAttempts=0 #initialize                                                                                                                                                          
   # make dictionary to be JSON-ified and inserted into the Elastic Search DB as a document
   keys   = ["fm_date","status"]
   values = [int(f) if str(f).isdigit() else str(f) for f in monitorData]
   transferMonitorDict=dict(zip(keys,values))
   transferMonitorDict['endTime']=transferMonitorDict['fm_date']
   while True:
       try:
           documentType='transfer'
           logger.info("About to try to insert into ES with the following info:")
           logger.info('Server: "' + esServerUrl+'/'+esIndexName+'/'+documentType+'/_update' + '"')
           logger.info("Data: '"+json.dumps(transferMonitorDict)+"'")
           monitorResponse=requests.post(esServerUrl+'/'+esIndexName+'/'+documentType+'/'+documentId+'/_update',data=json.dumps({"doc":transferMonitorDict}),timeout=1)
           if monitorResponse.status_code not in [200,201]:
               logger.error("elasticsearch replied with error code {0} and response: {1}".format(monitorResponse.status_code,monitorResponse.text))
           logger.info("{0}: Merger monitor produced response: {1}".format(datetime.now().strftime("%H:%M:%S"), monitorResponse.text))
           break
       except (requests.exceptions.ConnectionError,requests.exceptions.Timeout) as e:
           logger.error('elasticMonitorUpdate threw connection error: HTTP ' + monitorResponse.status_code)
           logger.error(monitorResponse.raise_for_status())
           if connectionAttempts > maxConnectionAttempts:
               logger.error('connection error: elasticMonitorUpdate failed to record '+documentType+' after '+ str(maxConnectionAttempts)+'attempts')
               break
           else:
               connectionAttempts+=1
               time.sleep(0.1)
           continue

#______________________________________________________________________________
def esMonitorMapping(esServerUrl,esIndexName):
# subroutine which creates index and mappings in elastic search database
   indexExists = False
   # check if the index exists:
   try:
      checkIndexResponse=requests.get(esServerUrl+'/'+esIndexName+'/_stats')
      if '_shards' in json.loads(checkIndexResponse.text):
         logger.info('found index '+esIndexName+' containing '+str(json.loads(checkIndexResponse.text)['_shards']['total'])+' total shards')
         indexExists = True
      else:
         logger.info('did not find existing index '+esIndexName)
         indexExists = False
   except requests.exceptions.ConnectionError as e:
      logger.error('esMonitorMapping: Could not connect to ElasticSearch database!')
   if indexExists:
      # if the index already exists, we put the mapping in the index for redundancy purposes:
      # JSON follows:
      transfer_mapping = {
         'transfer' : {
            '_all'          :{'enabled':False},  
            'properties' : {
               'fm_date'       :{'type':'date'}, #timestamp of injection
               'startTime'     :{'type':'date'}, #timestamp of transfer start
               'endTime'       :{'type':'date'}, #timestamp of transfer end
               'appliance'     :{'type':'keyword'},
               'host'          :{'type':'keyword'}, 
               'stream'        :{'type':'keyword'},
               'ls'            :{'type':'integer'},
               'processed'     :{'type':'integer'},
               'accepted'      :{'type':'integer'},
               'errorEvents'   :{'type':'integer'},
               'size'          :{'type':'long'},
               'runNumber'     :{'type':'integer'},
               'type'          :{'type':'keyword'}, #can be used for: Tier0, DQM, Error, LookArea, Calib, Special or similar
               'status'        :{'type':'integer'}  #transfer is started or finished
            }
         }
      }
      try:
         logger.info('esMonitorMapping: ' + esServerUrl+'/'+esIndexName+'/_mapping/transfer')
         logger.info('esMonitorMapping: ' + json.dumps(transfer_mapping))
         putMappingResponse=requests.put(esServerUrl+'/'+esIndexName+'/_mapping/transfer',data=json.dumps(transfer_mapping))
      except requests.exceptions.ConnectionError as e:
         logger.error('esMonitorMapping: Could not connect to ElasticSearch database!')
