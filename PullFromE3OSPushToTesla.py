__author__ = 'hal'

import datetime
import pymssql
import requests
import json
import os


class E3OSHelper():

    def __init__(self ):

        self.e3osSqlHost = os.environ['E3OS_SQL_HOST']
        self.e3osSqlUser = os.environ['E3OS_SQL_USER']
        self.e3osSqlPassword = os.environ['E3OS_SQL_PASSWORD']

        self.headers = {'content-type': 'application/json'}


    # ========= GET Data from SQL ==================================
    def getE3OSPointsList(self, e3osStationId):


        queryString = '''
            use oemvm;
            SELECT [DataPointName]
                  ,[DataPointXID]
                  ,[PointType]
                  ,[CreateTime]
                  ,[DisplayName]
              FROM [oemvm].[dim].[DataPoint_List]
              where StationID=%s
        ''' % e3osStationId

        try:
            conn = pymssql.connect(host=self.e3osSqlHost, user=self.e3osSqlUser,
                                   password=self.e3osSqlPassword, as_dict=True, database='oemvmdata')

            cur = conn.cursor()
            cur.execute(queryString)

            rowCount = 0
            rows = []
            for row in cur:
                rowCount += 1
                rows.append(row)

        except Exception, e:
            logError("could not connect to sql: " + str(e))

        finally:
            conn.close()

        return rows

    def private_getTableString(self, qualifier, xids):
        index = 1
        tableString = ""

        for xid in xids:
            if len(tableString) > 0:
                tableString += ' union all \n '

            # rowString = "select %s, '%s%s'" % ( str(index), qualifier, e3osPointName )
            rowString = "select " + str(index) + ", '" + xid + "'"
            index += 1
            tableString += rowString

        return tableString

    def getDataInRows(self, e3osPointNames, fromDateString, toDateString):

        queryString = '''
        use oemvmdata;
        declare @DataPointsOfInterest fact.datapointsofinterest
            insert @DataPointsOfInterest (seqNbr, DataPointXID)

        %s

        exec fact.DataSeriesGet2 @DataPointsOfInterest = @DataPointsOfInterest
           , @FromTime_Local = '%s'
           , @ToTime_Local = '%s'
           , @TimeInterval = 'minute'
           , @UserName = 'tkitchen'
           , @IncludeOutOfBounds = false
           , @IncludeUncommissioned = true
           , @UserId = '1d355ea9-7b16-4822-9483-1dd34173b5b8'
           , @TimeRange = null
        delete @DataPointsOfInterest
        ''' % (self.private_getTableString(e3osPointNames), fromDateString, toDateString)

        try:
            conn = pymssql.connect(host=self.e3osSqlHost, user=self.e3osSqlUser,
                                   password=self.e3osSqlPassword, as_dict=True, database='oemvmdata')

            cur = conn.cursor()
            cur.execute(queryString)

            rowCount = 0
            rows = []
            for row in cur:
                rowCount += 1
                rows.append(row)

        except Exception, e:
            logError("could not connect to sql: " + str(e))

        finally:
            conn.close()

        return self.private_transformRowData( len(xids), rows)

    # ============= Transform =======================

    def private_transformRowData(self, numPoints, rows):

        timeStampsAndValues = {}
        timeStamps = []

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

    '''
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
    '''

    # ===============================


class TeslaHelperClass():

    def __init__(self ):

        self.tesla_host = os.environ['TESLA_HOST']
        self.tesla_user = os.environ['TESLA_EMAIL']
        self.tesla_password = os.environ['TESLA_PASSWORD']
        self.stationId = os.environ['TESLA_STATION_ID']

        url = self.tesla_host + "/oauth/token"

        headers = {}
        headers['content-type'] = 'application/json'

        dict = {}
        dict['grantType'] = 'password'
        dict['email'] = self.tesla_user
        dict['password'] = self.tesla_password

        payload = json.dumps(dict)
        payload = json.loads(payload)

        try:
            resp = requests.post(url, json=payload, headers=headers)
            dict = json.loads(resp.text)

        except Exception, e:
            print e

        if resp.status_code != 200:
            msg = "Can't get token! host= " + self.tesla_host
            self.fatalError(msg, resp)

        self.tok = dict['accessToken']

    def setToken(self):
        url = self.tesla_host + "/oauth/token"

        headers = {}
        headers['content-type'] = 'application/json'

        dict = {}
        dict['grantType'] = 'password'
        dict['email'] = self.tesla_user
        dict['password'] = self.tesla_password

        payload = json.dumps(dict)
        payload = json.loads(payload)

        try:
            resp = requests.post(url, json=payload, headers=headers)
            dict = json.loads(resp.text)

        except Exception, e:
            print e

        if resp.status_code != 200:
            msg = "Can't get token! host= " + self.tesla_host
            self.fatalError(msg, resp)

        self.tok = dict['accessToken']

    def getTeslaPointsList(self):

        headers = {}
        headers['Accept'] = 'application/json'
        headers['content-type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.tok

        url = self.tesla_host + '/stations/%s/data-points' % self.stationId

        resp = requests.get(url, headers=headers)

        if resp.status_code != 200:
            print "Could not get points"

        listOfPoints = json.loads(resp.text)

        return listOfPoints


    def getNameToIdMap(self, datapoints):

        map = {}
        for dp in datapoints:
            map[dp['shortName']] = dp['id']

        return map


    def createMappingTable(self, e3osPointsList, teslaPointsList ):
        mappingTable = []

        for e3osPoint in e3osPointsList:
            #if not e3osPoint['PointType'] == 'BAS' :
            #    continue
            mappingTableRow = {}
            mappingTableRow['status'] = 'B_noTesla'
            mappingTableRow['e3osPointName'] = e3osPoint['DataPointName']
            mappingTableRow['e3osType'] = e3osPoint['PointType']
            mappingTableRow['xid'] = e3osPoint['DataPointXID']
            mappingTableRow['telsaPointName'] = ''
            mappingTableRow['telsaPointType'] = ''
            mappingTable.append(mappingTableRow)


        for teslaPoint in teslaPointsList:
            foundIt = False
            for mtRow in mappingTable:
                if mtRow['e3osPointName'] == teslaPoint['shortName']:
                    mtRow['status'] = 'A_mapped'
                    mtRow['telsaPointName'] = teslaPoint['shortName']
                    mtRow['telsaPointType'] = teslaPoint['type']
                    foundIt = True

            if not foundIt:
                mappingTableRow = {}
                mappingTableRow['status'] = 'C_noE3OS'
                mappingTableRow['e3osPointName'] = ''
                mappingTableRow['e3osType'] = ''
                mappingTableRow['xid'] = ''
                mappingTableRow['telsaPointName'] = teslaPoint['shortName']
                mappingTableRow['telsaPointType'] = teslaPoint['type']
                mappingTable.append(mappingTableRow)


        return mappingTable

    def postHistory(self, e3osMappedPointNames, timestamps, timeStampsAndValues):

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

            for pointName in e3osMappedPointNames:

                teslaId = teslaPointNameToIdMap[pointName]

                temp = valuesForAllPoints[pointIndex]

                if temp is not None:
                    if isinstance(temp, bool):
                        if temp > 0:
                            val = 1.0
                        else:
                            val = 0.0
                    elif isinstance(temp, str):
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


        payload = json.dumps(points)
        payload = json.loads(payload)
        headers = {}
        headers['Accept'] = 'application/json'
        headers['content-type'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.tok

        url = self.tesla_host + '/data/upsert'

        resp = requests.post(url, json=payload, headers=headers)

        # self.printPostInfo(url, headers, payload)

        if resp.status_code >= 300:
            self.setToken()
            resp = requests.post(url, json=payload, headers=headers)
            if resp.status_code >= 300:
                print "Could not post slice - ", resp.status_code
                print str(points)


    '''  
    def postSparsePointValues(self):
        
    MinimumChilledWaterFlow("MinimumChilledWaterFlow", "MinimumChilledWaterFlow"), 606.0
    TotalCapacity("TotalCapacity", "TotalCapacity"), 1400.0
    UtilityRate("BlendedUtilityRate", "UtilityRate"), 0.105
    CO2Rate("CO2EmissionFactor", "CO2Rate"); 1.0547
    '''



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


    fromDateString = os.environ['FROM_DATE']
    toDateString = os.environ['TO_DATE']
    e3osStationId = os.environ['E3OS_STATION_ID']

    #=====================================================================

    print 'Connecting to sql...'
    e3oshelper = E3OSHelper()

    print 'Getting datapoints from e3os'
    e3osPointsList = e3oshelper.getE3OSPointsList(e3osStationId)

    print 'Getting auth token from tesla...'
    teslaHelper = TeslaHelperClass()

    print 'Getting points list from tesla...'
    teslaPointsList = teslaHelper.getTeslaPointsList()
    teslaPointNameToIdMap = teslaHelper.getNameToIdMap(teslaPointsList)

    print 'create mapping table...'
    mappingTable = teslaHelper.createMappingTable(e3osPointsList, teslaPointsList)

    print 'status,e3osPointName,e3osType,telsaPointName,telsaPointType'
    for mtRow in mappingTable:
        print mtRow['status'] + ',' + mtRow['e3osPointName'] + ',' + mtRow['e3osType'] + ',' + mtRow[
            'telsaPointName'] + ',' + mtRow['telsaPointType']

    e3osMappedPointNames = []
    xids = []
    for mtRow in mappingTable:
        if mtRow['status'] == 'A_mapped' and mtRow['telsaPointType'] == 'raw':
            e3osMappedPointNames.append(mtRow['e3osPointName'])
            xids.append(mtRow['xid'])

    print 'Getting data from e3os...'
    timestamps, timestampsAndValues = e3oshelper.getDataInRows(xids, fromDateString, toDateString)
    if len(timestamps) <= 0:
        logError('no data from sql')


    #=========================================================================

    pushStartTime = datetime.datetime.now()
    pushStartTimeString = pushStartTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print 'Starting push at: ' + pushStartTimeString

    print 'Posting data...'
    teslaHelper.postHistory(e3osMappedPointNames, timestamps, timestampsAndValues)

    pushEndTime = datetime.datetime.now()
    pushEndTimeString = pushEndTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print 'Data push finised at: ' + pushEndTimeString

    print(pushEndTime - pushStartTime)

    print 'Done.'

