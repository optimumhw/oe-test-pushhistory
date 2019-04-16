__author__ = 'hal'

import datetime
import pymssql
import requests
import json
import urllib
import os
import xlrd
import sys


class ConfigurationHelper:
    def __init__(self):

        try:
            self.configDirPath = os.environ['EDGE_PYTHON_SCRIPTS_CONFIG_HOME'].strip()
        except:
            print

            self.configDirPath = '/Users/halwilkinson/Desktop/EdgePythonScriptsConfig'  # home
            # self.configDirPath = '/Users/hal/Desktop/PythonScriptConfigDir' #work
            print 'could not read EDGE_PYTHON_SCRIPTS_CONFIG_HOME env variable. using instead: ' + self.configDirPath
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
            logError('config.txt file i/o error. quitting')

    def getConfigSettings(self):
        return self.configSettings

    def getConfigDirPath(self):
        return self.configDirPath


class E3OSHelper():

    def __init__(self, configSettings, configDirPath, pointsListFileName):

        self.e3osSqlHost = os.environ['E3OS_SQL_HOST']
        self.e3osSqlUser = os.environ['E3OS_SQL_USER']
        self.e3osSqlPassword = os.environ['E3OS_SQL_PASSWORD']

        self.configDirPath = configDirPath
        self.headers = {'content-type': 'application/json'}

        excelFilPath = os.path.join(self.configDirPath, pointsListFileName)
        workbook = xlrd.open_workbook(excelFilPath)
        pointNameSheet = workbook.sheet_by_name('Points')

        self.pointsNameAndTypeList = []
        isFirstRow = True
        for rowNumber in range(0, pointNameSheet.nrows):

            # skip the header row - make sure there is a  header row
            if isFirstRow:
                isFirstRow = False
                continue

            includeCellObj = pointNameSheet.cell(rowNumber, 0)
            e3osPointNameCellObj = pointNameSheet.cell(rowNumber, 1)
            pointTypeCellObj = pointNameSheet.cell(rowNumber, 2)
            edisonPointNameCellObj = pointNameSheet.cell(rowNumber, 3)

            includeFlag = includeCellObj.value
            e3osPointName = e3osPointNameCellObj.value
            pointType = pointTypeCellObj.value
            edisonPointName = edisonPointNameCellObj.value

            if includeFlag == 0:
                continue

            dict = {}
            if e3osPointName is None or len(e3osPointName) == 0:
                dict['e3osPointName'] = edisonPointName
            else:
                dict['e3osPointName'] = e3osPointName

            dict['pointType'] = pointType
            dict['edisonPointName'] = edisonPointName

            if len(dict['edisonPointName']) <= 0:
                print "empty line at row: ", rowNumber

            else:
                self.pointsNameAndTypeList.append(dict)

    def getPointsAndTypesFromExcel(self):
        return self.pointsNameAndTypeList

    # ========= GET Data from SQL ==================================
    def private_getTableString(self, qualifier, pointsAndTypes):
        index = 1
        tableString = ""

        for pointAndType in pointsAndTypes:
            if len(tableString) > 0:
                tableString += ' union all \n '

            pointName = pointAndType['e3osPointName']

            # rowString = "select %s, '%s%s'" % ( str(index), qualifier, pointName )
            rowString = "select " + str(index) + ", '" + qualifier + pointName + "'"
            index += 1
            tableString += rowString

        return tableString

    def getDataInRows(self, qualifier, pointsAndTypes, fromDateString, toDateString):

        queryString = '''
        declare @DataPointsOfInterest fact.datapointsofinterest
            insert @DataPointsOfInterest (seqNbr, DataPointXID)

        %s

        exec fact.DataSeriesGet2 @DataPointsOfInterest = @DataPointsOfInterest
           , @FromTime_Local = '%s'
           , @ToTime_Local = '%s'
           , @TimeInterval = 'minute'
           , @UserName = 'tkitchen'
           , @IncludeOutOfBounds = false
           , @IncludeUncommissioned = false
           , @UserId = '1d355ea9-7b16-4822-9483-1dd34173b5b8'
           , @TimeRange = null
        delete @DataPointsOfInterest
        ''' % (self.private_getTableString(qualifier, pointsAndTypes), fromDateString, toDateString)

        try:
            conn = pymssql.connect(host=self.e3osSqlHost, user=self.e3osSqlUser,
                                   password=self.e3osSqlPassword, as_dict=True, database='oemvmdata')
        except Exception, e:
            logError("could not connect to sql: " + str(e))
        cur = conn.cursor()
        cur.execute(queryString)

        rowCount = 0
        rows = []
        for row in cur:
            rowCount += 1
            rows.append(row)

        conn.close()

        return self.private_transformRowData(rowCount, rows)

    # ============= Transform =======================

    def private_transformRowData(self, rowCount, rows):

        timeStampsAndValues = {}
        timeStamps = []

        pointsAndTypes = self.getPointsAndTypesFromExcel()
        numPoints = len(pointsAndTypes)

        for row in rows:

            index = row['id'] - 1
            tz = row['tz']  # TODO?: use timezone to convert to UTC
            ts = row['time']
            ts = ts.replace(second=0, microsecond=0)
            timestamp = ts.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

            val = row['value']

            if not timestamp in timeStampsAndValues:
                timeStamps.append(timestamp)
                temp = []
                for i in range(0, numPoints):
                    temp.append(None)
                timeStampsAndValues[timestamp] = temp

            vals = timeStampsAndValues[timestamp]
            vals[index] = val

        return (timeStamps, timeStampsAndValues)

    def dumpTimestampsAndValues(self, pointsAndTypes, timestamps, timeStampsAndValues):
        print 'Timestamp ',
        for pointNameAndType in pointsAndTypes:
            pointName = pointNameAndType['name']
            print pointName + " ",
        print

        for timestamp in timestamps:
            print timestamp,
            vals = timeStampsAndValues[timestamp]
            for val in vals:
                print val,
                print ' ',
            print

    # ===============================


class TeslaHelperClass():

    def __init__(self ):

        self.tesla_host = os.environ['TESLA_HOST']
        self.tesla_user = os.environ['TESLA_EMAIL']
        self.tesla_password = os.environ['TESLA_PASSWORD']
        self.stationId = os.environ['TESLA_STATION_ID']

        url = self.tesla_host + "/oauth/token"

        headers = {}
        headers['content-type'] = 'application/x-www-form-urlencoded'

        dict = {}
        dict['grant_type'] = 'password'
        dict['email'] = self.username
        dict['password'] = self.password

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

    def getTeslaPointsList(self):

        headers = {}
        headers['Accept'] = 'application/json'
        headers['content-type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.tok

        url = self.tesla_host + '/stations?%s' % self.stationId

        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            print "Could not get points"

        dict = json.loads(resp.text)

        datapoints = dict["dataPoints"]

        return datapoints


    def getNameToIdMap(self, datapoints):

        map = {}
        for dp in datapoints:
            map[dp[name]] = dp[id]

        return map


    def postHistory(self, pointsAndTypes, timestamps, timeStampsAndValues):

        teslaPointsList = self.getTeslaPointsList()
        teslaPointNameToIdMap = self.getNameToIdMap(teslaPointsList)

        print 'Checking that all points exist in tesla...'
        #tbd

        pointToTypeMap = {}
        for pt in pointsAndTypes:
            temp = pt['edisonPointName']
            if len(temp) == 0:
                temp = pt['e3osPointName']
            pointToTypeMap[temp] = pt['pointType']

        firstDay = None

        for timestamp in timestamps:

            thisSliceTime = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')

            if firstDay is None:
                firstDay = thisSliceTime

            if thisSliceTime > firstDay + datetime.timedelta(hours=1):
                firstDayString = firstDay.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

                print firstDayString + ' pushed'
                firstDay = thisSliceTime

            # '%Y-%m-%dT%H:%M:%S.%f'
            # '2017-01-16T00:00:00.000Z'

            valuesForAllPoints = timeStampsAndValues[timestamp]
            timestamps = []
            timestamps.append(timestamp)
            points = []
            pointIndex = 0

            maxPointsToPush = 50

            for pointAndTypeName in pointsAndTypes:

                pointName = pointAndTypeName['edisonPointName']
                if not pointName or len(pointName) == 0:
                    pointName = pointAndTypeName['e3osPointName']

                pointType = pointToTypeMap[pointName]

                teslaId = teslaPointNameToIdMap[pointName]

                if pointType != 'boolean' and pointType != 'numeric':
                    print 'yikes, bad point type for:', pointName, 'type:', pointType

                temp = valuesForAllPoints[pointIndex]

                if temp is not None:
                    if pointType == 'boolean':
                        if temp > 0:
                            val = 1.0
                        else:
                            val = 0.0
                    elif pointType == 'string':
                        val = temp
                    else:
                        val = float(temp)

                    dictPointAndValue = {}
                    dictPointAndValue["id"] = teslaId
                    dictPointAndValue["value"] = val
                    dictPointAndValue["timestamp"] = timestamp
                    points.append(dictPointAndValue)
                pointIndex += 1

                if len(points) >= maxPointsToPush:
                    self.private_postSlice(points)
                    points = []

            if len(points) > 0:
                self.private_postSlice(points)


    def private_postSlice(self, points):

        reqBody = {}
        reqBody['list_of_points'] = points
        payload = json.dumps(reqBody)

        headers = {}
        headers['Accept'] = 'application/json'
        headers['content-type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.tok

        url = self.host + '/data/upsert'

        resp = requests.post(url, data=payload, headers=headers)

        # self.printPostInfo(url, headers, payload)

        if resp.status_code != 200:
            print "Could not post slice - ", resp.status_code
            print str(points)

    def printPostInfo(self, url, headers, payload):
        print 'POST INFO'
        print '========='
        print 'url:'
        print url
        print 'headers:'
        print str(headers)
        print 'payload:'
        print str(payload)

    def fatalError(self, msg, resp):
        try:
            print msg, resp.status_code
            print str(resp.json())
        except:
            print msg
            pass
        exit()


# =================================================================
def logError(msg='error'):
    print msg
    exit()

if __name__ == '__main__':


    lastSuccessTimestamp = datetime.datetime.now()
    lastSuccessTimestampString = lastSuccessTimestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    configHelper = ConfigurationHelper()
    configSettings = configHelper.getConfigSettings()

    fromDateString = '2019-01-01T00:00:00.000Z'
    toDateString = '2010-01-15T23:59:55.000Z'
    pointsListFileName = 'BofA_HJ_PodA.xlsx'
    qualifier = 'BOA.BoAHJCP.BoAPDAEDGE.BoAPDAEDGE.'

    #=====================================================================

    print 'Connecting to sql...'
    e3oshelper = E3OSHelper(configSettings, configHelper.getConfigDirPath(), pointsListFileName)

    print 'Getting points list from Excel..'
    pointsAndTypes = e3oshelper.getPointsAndTypesFromExcel()
    if len(pointsAndTypes) <= 0:
        logError('pointsAndTypes dict is empty')

    print 'Getting data from e3os...'
    timestamps, timestampsAndValues = e3oshelper.getDataInRows(qualifier, pointsAndTypes, fromDateString, toDateString)
    if len(timestamps) <= 0:
        logError('no data from sql')


    #=========================================================================

    pushStartTime = datetime.datetime.now()
    pushStartTimeString = pushStartTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print 'Starting push at: ' + pushStartTimeString

    print 'Getting auth token...'
    teslaHelper = TeslaHelperClass()

    print 'Posting data...'
    teslaHelper.postHistory(pointsAndTypes, timestamps, timestampsAndValues)

    pushEndTime = datetime.datetime.now()
    pushEndTimeString = pushEndTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print 'Data push finised at: ' + pushEndTimeString

    print(pushEndTime - pushStartTime)

    print 'Done.'

