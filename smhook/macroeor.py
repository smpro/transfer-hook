#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
import shutil

logger = logging.getLogger(__name__)

def is_run_complete(
        theInputDataFolder,
        completeMergingThreshold,
        outputEndName,
        streamsToExclude,
        storeIniArea):
    """
    Defines if a run is complete.
    """
    theMergeMiniFolder  = "/store/lustre/mergeBU"
    theMergeMacroFolder = "/store/lustre/mergeMacro"

    if(outputEndName == ""):
        outputEndName = socket.gethostname()

    theRunNumber = ""

    inputDataFolderString = theInputDataFolder.split('/')
    if inputDataFolderString[len(inputDataFolderString) - 1] == '':
        theRunNumber = inputDataFolderString[len(inputDataFolderString) - 2]
    else:
        theRunNumber = inputDataFolderString[len(inputDataFolderString) - 1]

    theStoreIniArea = os.path.join(storeIniArea, theRunNumber)

    if not os.path.isdir(theStoreIniArea):
        logger.warning("Ini file folder {0} is not found, skipping the run".format(theStoreIniArea))
        return

    # reading the list of files in the given folder
    after = dict([(f, None) for f in os.listdir(theStoreIniArea)])
    afterStringIniNoSorted = [f for f in after]
    afterStringIni = sorted(afterStringIniNoSorted, reverse=False)

    iniIDict = dict()

    for nb in range(0, len(afterStringIni)):
        if afterStringIni[nb].endswith(".ini"):
            fileIniString = afterStringIni[nb].split('_')
            key = (fileIniString[2])

            if "DQM" in key:
                continue
            if "streamError" in key:
                continue
            if  key.replace("stream","") in streamsToExclude:
                continue

            if key in iniIDict.keys():
                iniIDict[key].append(fileIniString[3].split('.ini')[0])
            else:
                iniIDict.update({key: [fileIniString[3].split('.ini')[0]]})

    # reading the list of files in the given folder
    after = dict([(f, None) for f in os.listdir(theInputDataFolder)])
    afterStringSMNoSorted = [f for f in after]
    afterStringSM = sorted(afterStringSMNoSorted, reverse=False)

    numberMiniEoRFiles = 0
    eventsInputBUs = 0
    eventsInputFUs = 0
    eventsLostBUs = 0
    numberBoLSFiles = 0
    eventsTotalRun = 0
    eventsIDict = dict()

    # We try to get the last LS number first
    for nb in range(0, len(afterStringSM)):

        settingsLS = readFile(theInputDataFolder, afterStringSM[nb])

        if "bad" in settingsLS:
            continue

    for nb in range(0, len(afterStringSM)):

        settingsLS = readFile(theInputDataFolder, afterStringSM[nb])

        if "bad" in settingsLS:
            continue

        if ("MiniEoR" in afterStringSM[nb]):
            numberMiniEoRFiles += 1
            eventsInputBUs += int(settingsLS["eventsInputBU"])
            eventsInputFUs += int(settingsLS["eventsInputFU"])
            eventsLostBUs += int(settingsLS["eventsLostBU"])
            numberBoLSFiles = numberBoLSFiles + \
                int(settingsLS["numberBoLSFiles"])
            if(eventsTotalRun < int(settingsLS["eventsTotalRun"])):
                eventsTotalRun = int(settingsLS["eventsTotalRun"])

        else:
            eventsInput = int(settingsLS["data"][0])
            
            # 0: run, 1: ls, 2: stream
            fileNameString = afterStringSM[nb].split('_')
            key = (fileNameString[2])

            if "DQM" in key:
                continue
            if "streamError" in key:
                continue
            if  key.replace("stream","") in streamsToExclude:
                continue

            fillDictionary(key,eventsIDict,eventsInput)

    # Analyzing bad area
    theInputDataBadFolder = theInputDataFolder + "/bad"
    # reading the list of files in the given folder
    if os.path.exists(theInputDataBadFolder):
        afterBad = dict([(f, None) for f in os.listdir(theInputDataBadFolder)])
    else:
        afterBad = {}
    afterStringBad = [f for f in afterBad]

    eventsBadDict = dict()

    for nb in range(0, len(afterStringBad)):
        if not afterStringBad[nb].endswith(".jsn"):
            continue

        settingsLS = readFile(theInputDataBadFolder, afterStringBad[nb])

        if "bad" in settingsLS:
            continue

        eventsInput = int(settingsLS["data"][0])
        # 0: run, 1: ls, 2: stream
        fileNameString = afterStringBad[nb].split('_')
        key = (fileNameString[2])

        fillDictionary(key,eventsBadDict,eventsInput)
        fillDictionary(key,eventsIDict,eventsInput)

    # Analyzing the information
    isComplete = True
    eventsBuilt = eventsTotalRun - eventsLostBUs

    # Need at least one MiniEoRFile to be completed
    if numberMiniEoRFiles > 0:
        # Check if the number of bus per Stream coming from the ini files is 
        # consistent with the number of miniEoRFiles.
        for stream, nbus in iniIDict.items():
            if len(nbus) != numberMiniEoRFiles:
                ## TODO: Test and enable this logger.info call:
                # logger.info(
                #    'Run %s ' % theRunNumber.replace('run', '') +
                #    'is incomplete because nbus = %d ' % nbus +
                #    'and numberMiniEoRFiles = %d ' % numberMiniEoRFiles +
                #    'differ for stream %s!' % stream
                # )
                isComplete = False
        # Check if the number of Streams coming from the ini files is 
        # consistent with the number of merged Streams.
        if len(iniIDict.keys()) != len(eventsIDict.keys()) and eventsBuilt > 0:
            isComplete = False

        # Only go if it is still true
        if isComplete == True:
            for streamName in eventsIDict:                
                sumEvents = eventsIDict[streamName][0]
                if(sumEvents < eventsBuilt * completeMergingThreshold):
                    message = 'Run %(run)s is incomplete because ' + \
                        'sumEvents = %(sum)d is less than ' + \
                        'eventsBuilt * completeMergingThreshold = ' + \
                        '%(nbuilt)d * %(threshold)g = %(rhs).1f ' + \
                        'for stream %(stream)s'
                    substitutions = dict(
                        run    = theRunNumber.replace('run', ''),
                        sum = sumEvents,
                        nbuilt = eventsBuilt,
                        threshold = float(completeMergingThreshold),
                        rhs = float(eventsBuilt * completeMergingThreshold),
                        stream = streamName,
                        )
                    logger.info(message % substitutions)
                    isComplete = False
                elif(sumEvents > eventsBuilt):
                    isComplete = False
                    logger.warning(
                       "sumEvents > eventsBuilt!: {0} > {1}".format(
                        sumEvents,eventsBuilt
                       )
                    )

    else:
        ## TODO: Test and enable this logger.info call:
        # logger.info(
        #     'Run %s ' % theRunNumber.replace('run', '') +
        #     'is incomplete because there are no MiniEoR files!'
        # )
        isComplete = False

    logger.info(
        "run/events/completion: {0} {1} {2} {3} {4}".format(
            theInputDataFolder, eventsInputBUs, eventsInputFUs,
            numberBoLSFiles, isComplete
        )
    )
    if 'streamA' in iniIDict:
        logger.debug(
            "numberMiniEoRFiles/streamAfile: ".format(
                numberMiniEoRFiles, len(iniIDict["streamA"])
            )
        )

    # Deleting input folders, make sure you know what you are doing
    # It will not delete anything for now, just testing
    if isComplete == True and theRunNumber != "" and eventsBuilt > 0:
        remove_run_folder(theMergeMiniFolder, theRunNumber)
        remove_run_folder(theMergeMacroFolder, theRunNumber)

    EoRFileNameMacroOutput = theInputDataFolder + "/" + \
        theRunNumber + "_ls0000_MacroEoR_" + outputEndName + ".jsn_TEMP"
    EoRFileNameMacroOutputStable = theInputDataFolder + "/" + \
        theRunNumber + "_ls0000_MacroEoR_" + outputEndName + ".jsn"

    theEoRFileMacroOutput = open(EoRFileNameMacroOutput, 'w')
    theEoRFileMacroOutput.write(
        json.dumps(
            {
                'eventsInputBUs': eventsInputBUs,
                'eventsInputFUs': eventsInputFUs,
                'eventsStreamInput': eventsIDict,
                'eventsStreamBadInput': eventsBadDict,
                'numberBoLSFiles': numberBoLSFiles,
                'eventsTotalRun': eventsTotalRun,
                'eventsBuilt': eventsBuilt,
                'eventsLostBUs': eventsLostBUs,
                'numberMiniEoRFiles': numberMiniEoRFiles,
                'isComplete': isComplete
            },
            sort_keys=True, indent=4
        )
    )
    theEoRFileMacroOutput.close()

    shutil.move(EoRFileNameMacroOutput, EoRFileNameMacroOutputStable)

#_____________________________________________________________________________
def remove_run_folder(parent_folder, run_number):
    run_folder = os.path.join(parent_folder, run_number)
    if os.path.exists(run_folder):
        try:
            logger.info("Removing folder `{0}' ...".format(run_folder))
            #shutil.rmtree(run_folder)
        except Exception,e:
            logger.error("Failed to remove `{0}'!".format(run_folder))
            logger.exception(e)

#_____________________________________________________________________________
def readFile(theInputDataFolder, fileName):

    settingsLS = "bad"

    if not fileName.endswith(".jsn"):
        return settingsLS
    if "index" in fileName:
        return settingsLS
    if fileName.endswith("recv"):
        return settingsLS
    if "EoLS" in fileName:
        return settingsLS
    if "BoLS" in fileName:
        return settingsLS
    if "MacroEoR" in fileName:
        return settingsLS
    if "TransferEoR" in fileName:
        return settingsLS

    inputEoRJsonFile = os.path.join(theInputDataFolder, fileName)
    logger.debug("Inspecting `%s'" % inputEoRJsonFile)

    if(os.path.getsize(inputEoRJsonFile) > 0):
        try:
            settingsLS_textI = open(inputEoRJsonFile, "r").read()
            settingsLS = json.loads(settingsLS_textI)
        except Exception as e:
            logger.warning(
                "Looks like the file {0} ".format(inputEoRJsonFile)
                + "is not available, I'll try again..."
            )
            try:
                time.sleep(0.1)
                settingsLS_textI = open(inputEoRJsonFile, "r").read()
                settingsLS = json.loads(settingsLS_textI)
            except Exception as e:
                logger.warning(
                    "Looks like the file {0} ".format(inputEoRJsonFile)
                    + "is not available (2nd try)..."
                )
                time.sleep(1.0)
                settingsLS_textI = open(inputEoRJsonFile, "r").read()
                settingsLS = json.loads(settingsLS_textI)

    return settingsLS

#_____________________________________________________________________________
def fillDictionary(key,eventsDict,eventsInput):

    if key in eventsDict.keys():
        eventsInput = eventsDict[key][0] + eventsInput
        eventsDict[key].remove(eventsDict[key][0])
        eventsDict.update({key: [eventsInput]})

    else:
        eventsDict.update({key: [eventsInput]})
