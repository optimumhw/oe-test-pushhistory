__author__ = 'hal'

import time
import datetime
import pymssql
import requests
import json
import urllib
import os
import xlrd
import csv


class ConfigurationHelper:
    def __init__(self):

        try:
            self.configDirPath = os.environ['EDGE_PYTHON_SCRIPTS_CONFIG_HOME'].strip()
        except:
            print

            #self.configDirPath = '/Users/halwilkinson/Desktop/EdgePythonScriptsConfig'  #home
            self.configDirPath = '/Users/hal/Desktop/PythonScriptConfigDir' #work
            print 'could not read EDGE_PYTHON_SCRIPTS_CONFIG_HOME env variable. using:' + self.configDirPath
            pass

        try:
            configFilePath = os.path.join(self.configDirPath, 'config.txt')
            configFile = open(configFilePath, 'r')
            configLines = configFile.read().splitlines()
            configFile.close()

            self.configSettings = {}
            for line in configLines:
                if len(line) <= 0:
                    continue
                keyAndValue = line.split('=')
                self.configSettings[keyAndValue[0]] = keyAndValue[1]

        except:
            logError('config.txt file i/o error')

    def getConfigSettings(self):
        return self.configSettings

    def getConfigDirPath(self):
        return self.configDirPath

class ExcelReaderClass():

    def __init__(self, configDirPath, pointsListFileName, tabName):

        self.configDirPath = configDirPath
        excelFilPath = os.path.join(self.configDirPath, pointsListFileName)
        workbook = xlrd.open_workbook(excelFilPath)
        pointNameSheet = workbook.sheet_by_name(tabName)

        self.pointsNameAndTypeList = []
        isFirstRow = True
        for rowNumber in range(0, pointNameSheet.nrows):

            if isFirstRow:
                isFirstRow = False
                continue

            includeCellObj = pointNameSheet.cell(rowNumber, 0)
            pointNameCellObj = pointNameSheet.cell(rowNumber, 1)
            labelCellObj = pointNameSheet.cell(rowNumber, 2)
            sidCellObj = pointNameSheet.cell(rowNumber, 3)
            uomCellObj = pointNameSheet.cell(rowNumber, 5)

            includeFlag = includeCellObj.value
            pointName = pointNameCellObj.value
            label = labelCellObj.value
            sid = sidCellObj.value
            uom = uomCellObj.value

            if includeFlag == 0:
                continue

            dict = {}

            dict['pointName'] = pointName
            dict['label'] = label
            dict['sid'] = sid

            if uom == 'null':
                continue

            if uom == 'boolean':
                pointType = 'boolean'
            else:
                pointType = 'numeric'

            dict['pointType'] = pointType

            self.pointsNameAndTypeList.append(dict)


class EdgeHelperClass():

    def __init__(self, envType, configSettings, stationId, stationName, sendingStationName, lastSuccessTimestamp):

        self.stationId = stationId
        self.stationName = stationName
        self.sendingStationName = sendingStationName
        self.lastSuccessTimestamp = lastSuccessTimestamp

        if envType == 'PROD':
            self.host = configSettings['PROV_PROD_HOST']
            self.username = configSettings['PROV_PROD_USER']
            self.password = configSettings['PROV_PROD_PASSWORD']
        elif envType == 'OEDEV':
            self.host = configSettings['PROV_OEDEV_HOST']
            self.username = configSettings['PROV_OEDEV_USER']
            self.password = configSettings['PROV_OEDEV_PASSWORD']
        elif envType == 'OMNIBUS':
            self.host = configSettings['PROV_OMNIBUS_HOST']
            self.username = configSettings['PROV_OMNIBUS_USER']
            self.password = configSettings['PROV_OMNIBUS_PASSWORD']
        else:
            self.fatalError('no such type: ' + envType, None)

        url = self.host + "/auth/oauth/token"

        headers = {}
        headers['content-type'] = 'application/x-www-form-urlencoded'

        dict = {}
        dict['grant_type'] = 'password'
        dict['username'] = self.username
        dict['password'] = self.password
        dict['scope'] = 'read+write'

        payload = urllib.urlencode(dict)

        try:
            resp = requests.post(url, data=payload, headers=headers)
            dict = json.loads(resp.text)

        except Exception, e:
            print e

        if resp.status_code != 200:
            msg = "Can't get token! host= " + self.host
            self.fatalError(msg, resp)

        self.tok = dict['access_token']

    def getHistory(self, sid, startDate, endDate, resolution, pointsAndTypes):

        url = self.host + '/datapoint-histories?sid=%s&startDate=%s&endDate=%s&resolution=%s&sparse=false' % ( sid, startDate, endDate, resolution )

        for tempDict in pointsAndTypes:
            pointName = tempDict['pointName']
            url += '&names=' + pointName

        headers = {}
        headers['Accept'] = 'application/json'
        headers['content-type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.tok

        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            print "Could not get existing data"

        listOfPointNamesTimesAndValues = json.loads(resp.text)


        return listOfPointNamesTimesAndValues

    def private_postSlice(self, points ):

        reqBody = {}
        reqBody['timestamp'] = self.lastSuccessTimestamp
        reqBody['points'] = points
        reqBody['stationId'] = self.stationId
        reqBody['stationName'] = self.stationName
        reqBody['sendingStationName'] = self.sendingStationName
        reqBody['lastSuccessTimestamp'] = self.lastSuccessTimestamp
        payload = json.dumps(reqBody)

        headers = {}
        headers['Accept'] = 'application/json'
        headers['content-type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.tok

        url = self.host + '/stations/datapoint-history'

        resp = requests.post(url, data=payload, headers=headers)

        if resp.status_code != 200:
            print "Could not post slice"

    def processChunk(self, sid, startDateString, endDateString, resolution, pointsAndTypes ):

        pointNameAndTypeMap = {}
        for pointAndType in pointsAndTypes:
            pointNameAndTypeMap[ pointAndType['pointName']] = pointAndType['pointType']

        listOfPointNamesTimesAndValues = self.getHistory(
            sid, startDateString, endDateString, resolution, pointsAndTypes)

        for pointNameTimesAndValues in listOfPointNamesTimesAndValues:

            temper = pointNameTimesAndValues['name']
            if temper != 'CH1kW':
                continue

            timestamps = pointNameTimesAndValues['timestamps']
            if len( timestamps ) > 0 :

                values = pointNameTimesAndValues['values']

                newValues = []
                for tempVal in values:
                    newValues.append(None)

                pointName = pointNameTimesAndValues['name']

                aPoint = {}
                aPoint['name'] = pointName
                aPoint['pointType'] = pointNameAndTypeMap[ pointName ]
                aPoint['timestamps'] = timestamps
                aPoint['values'] = newValues
                aPoint['path'] = 'foo/bar'

                points = []
                points.append(aPoint)

                self.private_postSlice(points)


    def fatalError(self, msg, resp):
        try:
            print msg, resp.status_code
            print str(resp.json())
        except:
            print msg
            pass
        exit()




#=================================================================
def logError( msg = 'error' ):
    print msg
    print 'example usage: python CloneUsers.py PROD OEDEV'
    exit()


if __name__ == '__main__':

    #envType = 'OEDEV'
    #envType = 'PROD'
    #envType = 'OMNIBUS'

    #sid = 'c:customerdemo.s:demosite-edge.st:1'
    #startDateString = '2016-11-01T00:00:00.000Z'
    #endDateString = '2016-11-30T23:59:59.000Z'


    #stationID = 'demosite_016e29b3'
    #stationName = 'demosite_016e29b3'
    #sendingStationName = 'demosite_016e29b3'

    #pointsListFileName = 'DemoOnDev_StationPoints.xlsx'
    #tabName = 'Points'


    #========== DEMO on OEDEV ===========
    fromDateString = '2017-01-15T00:00:00.000Z'
    toDateString = '2017-01-31T23:59:55.000Z'
    envType = 'OEDEV'
    stationSid = 'c:whatcomindustries.s:demo-edge.st:1'
    stationID = 'demo_f0258b66'
    stationName = 'demo_f0258b66'
    sendingStationName = 'demo_f0258b66'
    pointsListFileName = 'DemoOnDev_StationPoints.xlsx'
    tabName = 'Points'

    #============ DEMO2 on PROD =======================
    #fromDateString = '2017-02-01T00:00:00.000Z'
    #toDateString = '2017-02-02T23:59:55.000Z'
    #envType = 'PROD'
    #stationSid = 'c:testcustomerdemo.s:demo2-edge.st:1'
    #stationID = 'demo2_358025ae'
    #stationName = 'demo2_358025ae'
    #sendingStationName = 'demo2_358025ae'
    #pointsListFileName = 'TEST_DemoOnDev_StationPoints.xlsx'
    #tabName = 'Points'
    #========================================

    #============ Omnibus =======================
    #fromDateString = '2017-03-01T00:00:00.000Z'
    #toDateString = '2017-03-31T23:59:55.000Z'
    #envType = 'OMNIBUS'
    #stationSid = 'c:greenstreetindustries.s:greentree-edge.st:1'
    #stationID = 'greentree_803b03de'
    #stationName = 'greentree_803b03de'
    #sendingStationName = 'greentree_803b03de'
    #pointsListFileName = 'GreenStreetOnOmnibusPoints.xlsx'
    #tabName = 'Points'
    #========================================

    resolution = 'fiveMinutes'


    #===========================

    # '2017-02-08T18:32:09.131Z'
    lastSuccessTimestamp = datetime.datetime.now()
    lastSuccessTimestampString = lastSuccessTimestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +'Z'

    print 'Getting config info...'
    configHelper = ConfigurationHelper()
    configSettings = configHelper.getConfigSettings()

    excelDataReader = ExcelReaderClass(configHelper.configDirPath, pointsListFileName, tabName)
    pointsAndTypes = excelDataReader.pointsNameAndTypeList

    print 'Getting auth token...'
    edgeHelper = EdgeHelperClass(envType, configSettings, stationID, stationName, sendingStationName, lastSuccessTimestampString)

    print 'Getting existing edge data...'
    startDate = datetime.datetime.strptime(fromDateString, "%Y-%m-%dT%H:%M:%S.%fZ")
    endDate =  datetime.datetime.strptime(toDateString, "%Y-%m-%dT%H:%M:%S.%fZ")

    print 'Posting null values...'

    # --------------------
    pushStartTime= datetime.datetime.now()
    pushStartTimeString = pushStartTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +'Z'
    print 'Starting push at: ' + pushStartTimeString

    # ---------------------
    tempStartDate = startDate
    while tempStartDate <= endDate:

        tempEndDate = tempStartDate + datetime.timedelta(days=1)

        tempStartDateString = tempStartDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        tempEndDateString = tempEndDate.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        edgeHelper.processChunk( stationSid, tempStartDateString, tempEndDateString, resolution, pointsAndTypes)
        tempStartDate = tempEndDate

    # ----------------------
    pushEndTime = datetime.datetime.now()
    pushEndTimeString = pushEndTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print 'Data push finised at: ' + pushEndTimeString
    print( pushEndTime - pushStartTime )
    print 'Done.'







