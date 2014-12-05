#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
import shutil

log = logging.getLogger(__name__)


def is_run_complete(
        debug,
        theInputDataFolder,
        completeMergingThreshold,
        outputEndName):
    """
    Defines if a run is complete.
    """

    if(outputEndName == ""):
        outputEndName = socket.gethostname()

    theRunNumber = ""

    inputDataFolderString = theInputDataFolder.split('/')
    if inputDataFolderString[len(inputDataFolderString) - 1] == '':
        theRunNumber = inputDataFolderString[len(inputDataFolderString) - 2]
    else:
        theRunNumber = inputDataFolderString[len(inputDataFolderString) - 1]

    # reading the list of files in the given folder
    after = dict([(f, None) for f in os.listdir(theInputDataFolder)])
    afterStringSM = [f for f in after]

    numberMiniEoRFiles = 0
    eventsInputBUs = 0
    eventsInputFUs = 0
    numberBoLSFiles = 0
    eventsTotalRun = 0
    eventsIDict = dict()
    iniIDict = dict()

    for nb in range(0, len(afterStringSM)):
        if afterStringSM[nb].endswith(".ini"):
            fileIniString = afterStringSM[nb].split('_')
            key = (fileIniString[2])
            if key in iniIDict.keys():
                iniIDict[key].append(fileIniString[3].split('.ini')[0])
            else:
                iniIDict.update({key: [fileIniString[3].split('.ini')[0]]})

        if not afterStringSM[nb].endswith(".jsn"):
            continue
        if "index" in afterStringSM[nb]:
            continue
        if afterStringSM[nb].endswith("recv"):
            continue
        if "EoLS" in afterStringSM[nb]:
            continue
        if "BoLS" in afterStringSM[nb]:
            continue
        if "MacroEoR" in afterStringSM[nb]:
            continue

        inputEoRJsonFile = os.path.join(theInputDataFolder, afterStringSM[nb])
        settingsLS = ""
        if(os.path.getsize(inputEoRJsonFile) > 0):
            try:
                settingsLS_textI = open(inputEoRJsonFile, "r").read()
                settingsLS = json.loads(settingsLS_textI)
            except ValueError as e:
                log.warning("Looks like the file {0} ".format(inputEoRJsonFile)
                            + "is not available, I'll try again...")
                try:
                    time.sleep(0.1)
                    settingsLS_textI = open(inputEoRJsonFile, "r").read()
                    settingsLS = json.loads(settingsLS_textI)
                except ValueError as e:
                    log.warning(
                        "Looks like the file {0} ".format(inputEoRJsonFile)
                        "is not available (2nd try)..."
                    )
                    time.sleep(1.0)
                    settingsLS_textI = open(inputEoRJsonFile, "r").read()
                    settingsLS = json.loads(settingsLS_textI)

        if ("MiniEoR" in afterStringSM[nb]):
            numberMiniEoRFiles += 1
            eventsInputBUs = eventsInputBUs + int(settingsLS["eventsInputBU"])
            eventsInputFUs = eventsInputFUs + int(settingsLS["eventsInputFU"])
            numberBoLSFiles = numberBoLSFiles + \
                int(settingsLS["numberBoLSFiles"])
            if(eventsTotalRun < int(settingsLS["eventsTotalRun"])):
                eventsTotalRun = int(settingsLS["eventsTotalRun"])

        else:
            eventsInput = int(settingsLS["data"][0])
            # 0: run, 1: ls, 2: stream
            fileNameString = afterStringSM[nb].split('_')
            key = (fileNameString[2])
            if key in eventsIDict.keys():

                eventsInput = eventsIDict[key][0] + eventsInput
                eventsIDict[key].remove(eventsIDict[key][0])
                eventsIDict.update({key: [eventsInput]})

            else:
                eventsIDict.update({key: [eventsInput]})

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

        inputBadJsonFile = os.path.join(
            theInputDataBadFolder,
            afterStringBad[nb])
        settingsLS = ""
        if(os.path.getsize(inputBadJsonFile) > 0):
            try:
                settingsLS_textI = open(inputBadJsonFile, "r").read()
                settingsLS = json.loads(settingsLS_textI)
            except ValueError as e:
                log.warning("Looks like the file {0} ".format(inputBadJsonFile)
                            + "is not available, I'll try again...")
                try:
                    time.sleep(0.1)
                    settingsLS_textI = open(inputBadJsonFile, "r").read()
                    settingsLS = json.loads(settingsLS_textI)
                except ValueError as e:
                    log.warning(
                        "Looks like the file {0} ".format(inputBadJsonFile)
                        + "is not available (2nd try)..."
                    )
                    time.sleep(1.0)
                    settingsLS_textI = open(inputBadJsonFile, "r").read()
                    settingsLS = json.loads(settingsLS_textI)

            eventsInput = int(settingsLS["data"][0])
            # 0: run, 1: ls, 2: stream
            fileNameString = afterStringBad[nb].split('_')
            key = (fileNameString[2])
            if key in eventsBadDict.keys():

                eventsInput = eventsBadDict[key][0] + eventsInput
                eventsBadDict[key].remove(eventsBadDict[key][0])
                eventsBadDict.update({key: [eventsInput]})

            else:
                eventsBadDict.update({key: [eventsInput]})

    # Analyzing the information
    isComplete = True
    for streamName in eventsIDict:
        if "DQM" in streamName:
            continue
        if "streamError" in streamName:
            continue
        sumEvents = eventsIDict[streamName][0]
        if streamName in eventsBadDict:
            sumEvents = sumEvents + eventsBadDict[streamName][0]
        if(sumEvents < eventsInputBUs * completeMergingThreshold):
            isComplete = False

    if(float(debug) >= 10):
        print "run/events/completion: ", theInputDataFolder, eventsInputBUs,
        print eventsInputFUs, numberBoLSFiles, isComplete
    if(float(debug) >= 10 and 'streamA' in iniIDict.keys()):
        print "numberMiniEoRFiles/streamAfile: ", numberMiniEoRFiles,
        print len(iniIDict["streamA"])

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
                'isComplete': isComplete}))
    theEoRFileMacroOutput.close()

    shutil.move(EoRFileNameMacroOutput, EoRFileNameMacroOutputStable)
