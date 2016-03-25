#this script creates the initial version info for one split test
#this script should be executed only once

#the version info table is the raw data for all activate versions
#the version id is unique and refers to a specific features combination

#the version tests table is a list of records (or transactions) tracking
#all version performance throughout the split-test
#note that the initial one will have the test ratio evenly across all versions


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd
from datetime import datetime
import csv


#define the function to simulate some versions
def getversioninfo(k=3):
    versionList = []
    timeFeature = ["morning",'afternoon',"evening"]
    titleFeature = ["short","mid","long"]
    #contentFeature = ["short","mid","long"]
    l = 0
    for i in range(k):
        for j in range(k):
            versionDict = {}
            versionDict["versionId"] = "version" + str(l)
            versionDict["timeFeature"] = timeFeature[i]
            versionDict["titleFeature"] = titleFeature[j]
            #versionDict["contentFeature"] = contentFeature[i/3]
            l += 1
            versionList.append(versionDict)
    return(pd.DataFrame(versionList))

#define the function to get initial split-test ratios with even odds
def gettestratio(versionInfo):
    n = len(set(versionInfo["versionId"]))
    ratio = float(1)/n
    return(ratio)

#define the function to get current time
def gettesttime():
    testTime = datetime.utcnow()
    return(testTime)

#define the function to get current date
def gettestdate(testTime):
    testDate = str(testTime.month) + '/' + str(testTime.day) + '/' + str(testTime.year)
    return(testDate)

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

versionInfo = getversioninfo()
#print versionInfo

#create the raw version info table as a reference
session.execute("""
CREATE TABLE IF NOT EXISTS "rawVersionInfo" (
    "versionId" varchar,
    "timeFeature" varchar,
    "titleFeature" varchar,
    PRIMARY KEY ("versionId")
)
""")

#insert raw data to cassandra table "rawVersionInfo"
print "inserting version info into cassandra, please wait about 1 second"
for i in range(versionInfo.shape[0]):
    values = versionInfo.iloc[i].values.tolist()
    prepared_stmt = session.prepare("""
    INSERT INTO "rawVersionInfo" ("timeFeature","titleFeature","versionId")
    VALUES (?, ?, ?)
    """)
    bound_stmt = prepared_stmt.bind(values)
    stmt = session.execute(bound_stmt)

#get initial split-test ratios
testRatio = gettestratio(versionInfo)
#get current date and time
testTime = gettesttime()
testDate = gettestdate(testTime)
#print testTime
#print testDate

#create the version test table for split-testing
session.execute("""
CREATE TABLE IF NOT EXISTS "versionTests" (
    "versionId" varchar,
    "timeFeature" varchar,
    "titleFeature" varchar,
    "testDate" varchar,
    "testTime" timestamp,
    visits int,
    clicks int,
    rate float,
    "testRatio" double,
    PRIMARY KEY (("versionId","testDate"))
)
""")

#insert initial version test data to cassandra table "versionTests"
print "inserting the version test list into cassandra, please wait about 1 second"
for i in range(versionInfo.shape[0]):
    values = versionInfo.iloc[i].values.tolist() + [testDate,testTime,None,None,None,testRatio]
    prepared_stmt = session.prepare("""
    INSERT INTO "versionTests" ("timeFeature","titleFeature","versionId","testDate","testTime",visits,clicks,rate,"testRatio")
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    bound_stmt = prepared_stmt.bind(values)
    stmt = session.execute(bound_stmt)
