#!/usr/bin/env python

import os, time, sys, getopt, fcntl, shutil, json, zlib, requests
import smhook.copyWorker as copyWorker

def write_json(jsnfile,mode):
    
    run, ls, stream = copyWorker.parse_filename(jsnfile)
    datfile = jsnfile.replace('.jsn', '.dat')

    if(mode == "cdaq"):
    #res = requests.post('http://es-cdaq.cms:9200/runindex_cdaq_read/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":[{"term":{"runNumber":'+str(run)+'}},{"term":{"ls":'+str(ls)+'}},{"term":{"stream":"'+stream+'"}}] }}}')
        res = requests.post('http://es-cdaq.cms:9200/runindex_cdaq_read/macromerge/_search?pretty&size=1000','{"query":{ "bool":{"must":[{"term":{"runNumber":'+str(run)+'}},{"term":{"ls":'+str(ls)+'}},{"term":{"stream":"'+stream+'"}}] }}}')
    else:
        res = requests.post('http://es-cdaq.cms:9200/runindex_minidaq_read/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":[{"term":{"runNumber":'+str(run)+'}},{"term":{"ls":'+str(ls)+'}},{"term":{"stream":"'+stream+'"}}] }}}')
    js = json.loads(res.content)

    print js

    hitlist = js['hits']['hits']#['_source']['in']
    print "statistics for run",run,"stream",stream,"ls",ls,"  number of BUs reporting mini-merge:",len(hitlist)
    tot = hitlist[0]['_source']['eolField2']

    totproc=0
    totacc =0
    size   =0

    for hit in hitlist:
        totproc+=hit['_source']['processed']
        totacc +=hit['_source']['accepted']
        size    =hit['_source']['size']

        print "  processed",totproc,"accepted",totacc,"   EoLS total:",tot

    eventsInput  = 0
    eventsOutput = 0
    eventsInput  = eventsInput  + totproc
    eventsOutput = eventsOutput + totacc

    theMergedJSONfile = open(jsnfile, 'w')
   
    theMergedJSONfile.write(json.dumps({'data': (eventsInput, eventsOutput, 0, datfile, size, 0, 0, tot, 0, "FailSafe")}))
    theMergedJSONfile.close()


write_json("run295810_ls0011_streamNanoDST_StorageManager.jsn","cdaq")
write_json("run295810_ls0014_streamCalibration_StorageManager.jsn","cdaq")
write_json("run295810_ls0006_streamPhysicsCommissioning_StorageManager.jsn","cdaq")
write_json("run295810_ls0009_streamALCALUMIPIXELS_StorageManager.jsn","cdaq")
write_json("run295810_ls0012_streamPhysicsCommissioning_StorageManager.jsn","cdaq")
write_json("run295810_ls0012_streamCalibration_StorageManager.jsn","cdaq")
