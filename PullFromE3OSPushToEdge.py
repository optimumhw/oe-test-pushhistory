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

            self.configDirPath = '/Users/halwilkinson/Desktop/EdgePythonScriptsConfig'  #home
            #self.configDirPath = '/Users/hal/Desktop/PythonScriptConfigDir' #work
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

    def __init__( self, configSettings, configDirPath, pointsListFileName):

        self.e3osSqlHost = configSettings['E3OS_SQL_HOST']
        self.e3osSqlUser = configSettings['E3OS_SQL_USER']
        self.e3osSqlPassword = configSettings['E3OS_SQL_PASSWORD']

        self.configDirPath = configDirPath
        self.headers = {'content-type': 'application/json'}

        excelFilPath = os.path.join(self.configDirPath, pointsListFileName)
        workbook = xlrd.open_workbook(excelFilPath)
        pointNameSheet = workbook.sheet_by_name('Points')

        self.pointsNameAndTypeList = []
        isFirstRow = True
        for rowNumber in range(0, pointNameSheet.nrows):

            #skip the header row - make sure there is a  header row
            if isFirstRow :
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

    #========= GET Data from SQL ==================================
    def private_getTableString(self, qualifier, pointsAndTypes):
        index = 1
        tableString = ""

        for pointAndType in pointsAndTypes:
            if len(tableString) > 0 :
                tableString += ' union all \n '

            pointName = pointAndType['e3osPointName']

            #rowString = "select %s, '%s%s'" % ( str(index), qualifier, pointName )
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
        ''' % (self.private_getTableString( qualifier, pointsAndTypes) , fromDateString, toDateString)

        try:
            conn = pymssql.connect(host=self.e3osSqlHost, user=self.e3osSqlUser,
                                   password=self.e3osSqlPassword, as_dict=True, database='oemvmdata')
        except Exception, e:
            logError( "could not connect to sql: " + str(e) )
        cur = conn.cursor()
        cur.execute( queryString )

        rowCount = 0
        rows = []
        for row in cur:
            rowCount += 1
            rows.append( row )

        conn.close()

        return self.private_transformRowData( rowCount, rows )


    #============= Transform =======================

    def private_transformRowData(self, rowCount, rows):

        timeStampsAndValues = {}
        timeStamps = []

        pointsAndTypes = self.getPointsAndTypesFromExcel()
        numPoints = len(pointsAndTypes)

        for row in rows:

            index = row['id'] - 1
            tz = row['tz'] #TODO?: use timezone to convert to UTC
            ts = row['time']
            ts = ts.replace(second=0,microsecond=0)
            timestamp = ts.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +'Z'

            val = row['value']

            if not timestamp in timeStampsAndValues:
                timeStamps.append(timestamp)
                temp = []
                for i in range (0, numPoints):
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

    #===============================


class EdgeHelperClass():

    def __init__(self, envType, configSettings, siteSid, stationId, stationName, sendingStationName, lastSuccessTimestamp):

        self.siteSid = siteSid
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

    def getPointsList(self):

        headers = {}
        headers['Accept'] = 'application/json'
        headers['content-type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.tok

        url = self.host + '/datapoints?sid=%s' % self.siteSid

        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            print "Could not get points"

        dict = json.loads(resp.text)

        return dict

    def getEdisonPointsToTypeMap(self, pointsList ):

        edisonPointsToTypeMap = {}
        for pointDict in pointsList:
            pointName = pointDict['name']
            v = pointDict['value']
            pointType = type(v).__name__

            edisonPointType = 'string'
            if pointType == 'int' :
                edisonPointType = 'numeric'
            elif pointType == 'float':
                edisonPointType = 'numeric'
            elif pointType == 'bool':
                edisonPointType = 'boolean'
            edisonPointsToTypeMap[ pointName ] = edisonPointType

        return edisonPointsToTypeMap


    def postHistory(self, pointsAndTypes, timestamps, timeStampsAndValues):

        edisonPointsList = self.getPointsList()
        edisonPointToTypeMap = self.getEdisonPointsToTypeMap( edisonPointsList)

        print 'Checking that all points exist in edison...'
        allPointsAreValid = True
        for pointAndTypeName in pointsAndTypes:
            pointName = pointAndTypeName['edisonPointName']
            if not pointName or len(pointName) == 0:
                pointName = pointAndTypeName['e3osPointName']

            try:
                edisonPointType = edisonPointToTypeMap[ pointName ]
            except:
                print 'could not find this point: ' + pointName
                allPointsAreValid = False
                continue

            excelType = pointAndTypeName['pointType']
            if edisonPointType != excelType:
                print 'pointTypeConflict:', pointName, edisonPointType, '(edison) vs', excelType, '(excel)'
                allPointsAreValid = False


        if not allPointsAreValid:
            logError('some points invalid')
        else:
            print 'all points are valid!'


        pointToTypeMap = {}
        for pt in pointsAndTypes:
            temp = pt['edisonPointName']
            if len(temp) == 0 :
                temp = pt['e3osPointName']
            pointToTypeMap[ temp ] = pt['pointType']

        firstDay = None

        for timestamp in timestamps:

            thisSliceTime = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')

            if firstDay is None :
                firstDay = thisSliceTime

            if thisSliceTime > firstDay + datetime.timedelta(hours=1):
                firstDayString = firstDay.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

                print firstDayString + ' pushed'
                firstDay = thisSliceTime

            #'%Y-%m-%dT%H:%M:%S.%f'
            #'2017-01-16T00:00:00.000Z'

            valuesForAllPoints = timeStampsAndValues[timestamp]
            timestamps = []
            timestamps.append( timestamp )
            points = []
            pointIndex = 0;

            maxPointsToPush = 50

            for pointAndTypeName in pointsAndTypes:

                pointName = pointAndTypeName['edisonPointName']
                if not pointName or len(pointName)==0:
                    pointName = pointAndTypeName['e3osPointName']

                pointType = pointToTypeMap[ pointName ]

                if pointType != 'boolean' and pointType != 'numeric':
                    print 'yikes, bad point type for:', pointName, 'type:', pointType

                temp = valuesForAllPoints[pointIndex]

                if temp is not None:
                    if pointType == 'boolean':
                        if temp > 0 :
                            val = True
                        else:
                            val = False
                    elif pointType == 'string':
                        val = temp
                    else:
                        val = float(temp)

                    values = []
                    values.append( val )
                    aPoint = {}
                    aPoint['name'] = pointName
                    aPoint['pointType'] = pointType
                    aPoint['timestamps'] = timestamps
                    aPoint['values'] = values
                    aPoint['path'] = 'foo/bar'
                    points.append(aPoint)
                pointIndex += 1

                if len( points ) >=  maxPointsToPush:
                    self.private_postSlice( points )
                    #for pt in points:
                    #    pts = []
                    #    pts.append(pt)
                    #    self.private_postSlice(pts)
                    points = []

            if len( points ) > 0:
                self.private_postSlice(points)
                #for pt in points:
                #    pts = []
                #    pts.append( pt )
                #    self.private_postSlice(pts)


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

        #self.printPostInfo(url, headers, payload)

        if resp.status_code != 200:
             print "Could not post slice - ", resp.status_code
             print str(points)

    def printPostInfo( self, url, headers, payload ):
        print 'POST INFO'
        print '========='
        print 'url:'
        print url
        print 'headers:'
        print str(headers)
        print 'payload:'
        print str(payload)

        dict = json.loads(payload)
        for k,v in dict.iteritems():

            if k == 'points':
               print 'POINTS:'
               pointArray = v
               pointIndex = 0
               for pointDict in pointArray:
                    print '   ', pointIndex, ":", pointDict['name'], pointDict['pointType'], '#ts:', len( pointDict['timestamps']), '#v:', len(pointDict['values'])
                    pointIndex += 1
            else:
                print k, " : ", v

        print '=== end of post info ==='


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
    exit()


if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print 'usage: python PullFromE3OSPushToEdge.py siteToLoad'
        sys.exit(0)

    siteToLoad = sys.argv[1]

    lastSuccessTimestamp = datetime.datetime.now()
    lastSuccessTimestampString = lastSuccessTimestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +'Z'

    configHelper = ConfigurationHelper()
    configSettings = configHelper.getConfigSettings()

    if siteToLoad == "BlueWater" :
        #========== Blue Water ===========
        fromDateString = '2017-07-01T00:00:00.000Z'
        toDateString = '2017-07-02T23:59:55.000Z'
        envType = 'OEDEV'
        stationSid = 'c:jacksonstreetindustries.s:bluewater-edge.st:1'
        stationID = 'bluewater_19fa9ff5'
        stationName = 'bluewater_19fa9ff5'
        sendingStationName = 'bluewater_19fa9ff5'
        pointsListFileName = 'bluewater_points.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'

    elif siteToLoad == "DEMO_OEDEV" :
        #========== DEMO on OEDEV ===========
        fromDateString = '2017-07-01T00:00:00.000Z'
        toDateString = '2017-08-22T23:59:55.000Z'
        envType = 'OEDEV'
        stationSid = 'c:testcustomerdemo.s:demo-edge.st:1'
        stationID = 'demo_f0258b66'
        stationName = 'demo_f0258b66'
        sendingStationName = 'demo_f0258b66'
        pointsListFileName = 'livepoints.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'

    elif siteToLoad == "SavingsDE":
        #========== SAVINGS DEMO on OEDEV ===========
        fromDateString = '2017-05-05T00:00:00.000Z'
        toDateString = '2017-09-13T23:59:55.000Z'
        envType = 'OEDEV'
        stationSid = 'c:testcustomerdemo.s:savingsde-edge.st:1'
        stationID = 'savingsde_2bfd576d'
        stationName = 'savingsde_2bfd576d'
        sendingStationName = 'savingsde_2bfd576d'
        pointsListFileName = 'livepoints.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'

    elif siteToLoad == "V2DemoSA":
        #============ v2DemoSavings on OEDEV =======================
        fromDateString = '2017-10-05T00:00:00.000Z'
        toDateString = '2017-10-08T23:59:55.000Z'
        envType = 'OEDEV'
        stationSid = 'c:testcustomerdemo.s:v2demosa-edge.st:1'
        stationID = 'v2demosa_aa3416f4'
        stationName = 'v2demosa_aa3416f4'
        sendingStationName = 'v2demosa_aa3416f4'
        pointsListFileName = 'livepoints.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'


    elif siteToLoad == "V5Demo":
        #============ v3DemoSavings on OEDEV =======================
        fromDateString = '2017-07-01T00:00:00.000Z'
        toDateString = '2017-07-31T23:59:55.000Z'
        envType = 'OEDEV'
        stationSid = 'c:testcustomerdemo.s:v5demosa-edge.st:1'
        stationID = 'v5demosa_8e4bef22'
        stationName = 'v5demosa_8e4bef22'
        sendingStationName = 'v5demosa_8e4bef22'
        pointsListFileName = 'livepoints.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'

    elif siteToLoad == "Demo2":
        #============ DEMO2 on PROD =======================
        fromDateString = '2017-10-04T00:00:00.000Z'
        toDateString = '2017-10-09T00:00:00.000Z'
        envType = 'PROD'
        stationSid = 'c:testcustomerdemo.s:demo2-edge.st:1'
        stationID = 'demo2_358025ae'
        stationName = 'demo2_358025ae'
        sendingStationName = 'demo2_358025ae'
        pointsListFileName = 'livepointsDemo2.xlsx'
        qualifier = 'JnJ.EthiconNM.EthiconNM.EthiconNM.'

    elif siteToLoad == "Demo2Sav":
        #============ DEMO2 on PROD =======================
        fromDateString = '2017-10-01T00:00:00.000Z'
        toDateString = '2017-12-05T00:00:00.000Z'
        envType = 'PROD'
        stationSid = 'c:savingscustomerdemo.s:demo2sav-edge.st:1'
        stationID = 'demo2sav_24911317'
        stationName = 'demo2sav_24911317'
        sendingStationName = 'demo2sav_24911317'
        pointsListFileName = 'livepointsDemo2.xlsx'
        qualifier = 'JnJ.EthiconNM.EthiconNM.EthiconNM.'

        
    elif siteToLoad == "Melia":
        #============ Melia-2 on PROD =======================
        fromDateString = '2017-10-01T00:00:00.000Z'
        toDateString = '2017-10-31T00:00:00.000Z'
        envType = 'PROD'
        stationSid = 'c:testcustomerdemo.s:melia2-edge.st:1'
        stationID = 'melia2_d8a54aab'
        stationName = 'melia2_d8a54aab'
        sendingStationName = 'melia2_d8a54aab'
        pointsListFileName = 'livepointsDemo2.xlsx'
        qualifier = 'JnJ.EthiconNM.EthiconNM.EthiconNM.'


    elif siteToLoad == "Atom":
        #============ Atom test =======================
        fromDateString = '2017-01-01T00:00:00.000Z'
        toDateString = '2017-01-26T23:59:55.000Z'
        envType = 'OMNIBUS'
        stationSid = 'c:greenstreetindustries.s:greentree-edge.st:1'
        stationID = 'greentree_803b03de'
        stationName = 'greentree_803b03de'
        sendingStationName = 'greentree_803b03de'
        pointsListFileName = 'VistakonAtomPoints.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'
    #hal is cool

    elif siteToLoad == "Demo1":
        #============Demo1onPROD=======================
        fromDateString = '2017-11-01T00:00:00.000Z'
        toDateString = '2017-11-02T23:59:55.000Z'
        envType = 'PROD'
        stationSid = 'c:testcustomerdemo.s:demo1-edge.st:1'
        stationID = 'demo1_09dd55c4'
        stationName = 'demo1_09dd55c4'
        sendingStationName = 'demo1_09dd55c4'
        pointsListFileName = 'livepointsDemo1_prod.xlsx'
        qualifier = 'THPH.THC.THCEDGE.THCEDGE.'

    else:
        logError('site to load: ' + siteToLoad + ' not found!')


    print 'Connecting to sql...'
    e3oshelper = E3OSHelper( configSettings, configHelper.getConfigDirPath(), pointsListFileName )

    print 'Getting auth token...'
    edgeHelper = EdgeHelperClass(envType, configSettings, stationSid, stationID, stationName, sendingStationName, lastSuccessTimestampString)

    print 'Getting points list from Excel..'
    pointsAndTypes = e3oshelper.getPointsAndTypesFromExcel( )
    if len( pointsAndTypes ) <= 0 :
        logError('pointsAndTypes dict is empty')

    print 'Getting data from e3os...'
    timestamps, timestampsAndValues = e3oshelper.getDataInRows( qualifier, pointsAndTypes, fromDateString, toDateString )
    if len(timestamps) <= 0 :
        logError('no data from sql')

    pushStartTime= datetime.datetime.now()
    pushStartTimeString = pushStartTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] +'Z'
    print 'Starting push at: ' + pushStartTimeString

    print 'Posting data...'
    edgeHelper.postHistory(pointsAndTypes, timestamps, timestampsAndValues)

    pushEndTime = datetime.datetime.now()
    pushEndTimeString = pushEndTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print 'Data push finised at: ' + pushEndTimeString

    print( pushEndTime - pushStartTime )

    print 'Done.'

