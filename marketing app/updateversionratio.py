#this script updates the version test ratio for each version
#this script should be executed periodically after collecting responses from sent emails

#note that we are using multi-armed bandit test, which is a extended version of A/B test
#to calculate the updated version test ratios

#the basic process is shown as below:
#first get the most recent conversion rates for all split versions
#then after applying Monte Carlo simulation for n = 10000 as default, get new test
#version ratios for the next split test, which are just the baysian probabilities
#finally calculate the remaining value for the splitting test to see whether we should
#terminate the process when one version is significantly better than others

#important: since the real baysian probabilities are too complex, we apply Monte Carlo
#simulation to make approximations. We use the beta distribution generator, Beta(k,n+1-k),
#where k = # of clicks (target metrics), n = # of total visits (total populations)

#after calculating the next test version ratios, we update the table 'versionTests'.
#from the printouts, we can get the info whether to continue the test or terminate it


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from numpy import random
from datetime import datetime
import pandas as pd


#define the function to check all inputs
def checkinputs(versionInfo,sentEmailList,minValidResponse):
    if len(versionInfo) < 1:
        raise ValueError("version info hasn't been correctly set up")
    else:
        print "we will update " + str(versionInfo) + " version test ratios"
    visits = sentEmailList.shape[0]
    clicks = sentEmailList.loc[sentEmailList.response != 'no'].shape[0]
    if clicks < minValidResponse:
        raise ValueError("not enough positive responses are collected")
    else:
        print "we now have sent " + str(visits) + " emails and get " + str(clicks) + " positive responses"

#define the function to get version test results, such as version id, # of visits and # of clicks
def getversiontestresults(sentEmailList,versionInfo):
    versionTestResultsList = []
    #get unique version ids
    versionIds = set(sentEmailList["versionId"])
    print versionIds
    for versionId in versionIds:
        versionTestResultsDict = {}
        versionVisits = sentEmailList.loc[sentEmailList.versionId == versionId]
        versionClicks = versionVisits.loc[versionVisits.response != 'no']
        versionTestResultsDict["versionId"] = versionId
        print versionId
        print versionInfo
        versionTestResultsDict["timeFeature"] = list(versionInfo.loc[versionInfo.versionId == versionId,"timeFeature"])[0]
        versionTestResultsDict["titleFeature"] = list(versionInfo.loc[versionInfo.versionId == versionId,"titleFeature"])[0]
        versionTestResultsDict["visits"] = versionVisits.shape[0]
        versionTestResultsDict["clicks"] = versionClicks.shape[0]
        if versionVisits.shape[0] != 0:
            versionTestResultsDict["rate"] = float(versionClicks.shape[0])/versionVisits.shape[0]
        else:
            versionTestResultsDict["rate"] = 0
        versionTestResultsList.append(versionTestResultsDict)
    return(versionTestResultsList)

#define the function to get ratios of next split test
def getnextsplit(versionTestResults,percentile,stopRatio,n=10000):
    #collect current convertion rates and MC results
    ratioList = []
    MCDict = {}
    for version in versionTestResults:
        #get current convertion rates for all versions
        versionId = version.get("versionId")
        visits = version.get("visits")
        clicks = version.get("clicks")
        #convertion rate = click / visit
        ratioList.append(float(clicks)/float(visits))
        MCRateList = []
        for i in range(n):
            if clicks == 0:
                MCRateList.append(0)
            else:
                MCRateList.append(random.beta(clicks,visits-clicks+1))
        MCDict[versionId] = MCRateList
    #check if we have at least one click
    if max(ratioList) == 0:
        raise ValueError("not enough valid responses from the sent email list")
    MCData = pd.DataFrame(MCDict)
    #get version id
    versionIds = list(MCData.columns.values)
    #get the best version based on the simulated values
    totalValues = []
    for versionId in versionIds:
        totalValues.append(sum(MCData[versionId]))
    bestVersionIndex = totalValues.index(max(totalValues))
    #convert simulation results from version id to index
    maxVersionIndex = []
    for maxVersionId in list(MCData.idxmax(axis = 1)):
        maxVersionIndex.append(versionIds.index(maxVersionId))
    MCData["max"] = maxVersionIndex
    #get next split test info
    #nextSplitList = []
    nextSplitDict = {}
    for versionId in versionIds:
        #nextSplitDict = {}
        k = sum(MCData["max"] == MCData.columns.get_loc(versionId))
        #print k
        #nextSplitDict["versionId"] = versionId
        #nextSplitDict["ratio"] = float(k)/float(n)
        nextSplitDict[versionId] = float(k)/float(n)
        #nextSplitList.append(nextSplitDict)
    #calculate the remaining value of this test
    remainingValue = getremainingvalue(MCData,bestVersionIndex,percentile,n)
    #check whether to stop this test when the remaining value is too small
    bestVersionRate = ratioList[bestVersionIndex]
    remainingValue = getstopinfo(bestVersionRate,remainingValue,stopRatio)
    #return({"nextSplitRatio":nextSplitList,"remainingValue":remainingValue})
    return({"nextSplitRatio":nextSplitDict,"remainingValue":remainingValue})

#define the function to get the remaining value
def getremainingvalue(MCData,bestVersionIndex,percentile,n):
    #calculate the remaining value
    remainingValueDict = {}
    remainingValueDict["bestVersion"] = list(MCData.columns.values)[bestVersionIndex]
    #pre-check if value = 0
    if MCData.loc[MCData["max"] == bestVersionIndex].shape[0] == n:
        remainingValue = 0
    else:
        #calculate the remaining value when the best one isn't the max one by definition
        remainingMCData = MCData.loc[MCData["max"] != bestVersionIndex]
        remainingValueList = []
        for i in remainingMCData.index.values:
            maxIndex = remainingMCData.ix[i,"max"]
            bestValue = remainingMCData.ix[i,bestVersionIndex]
            maxValue = remainingMCData.ix[i,maxIndex]
            remainingValueList.append((maxValue-bestValue)/bestValue)
        k = len(remainingValueList)
        remainingValueList = [0] * (n - k) + sorted(remainingValueList)
        remainingValue = remainingValueList[int(round(percentile*n))]
    #return as a dictionary with best version and its remaining value
    remainingValueDict["remainingValue"] = remainingValue
    return(remainingValueDict)

#define the function to determine whether we need to stop the process
def getstopinfo(bestVersionRate,remainingValue,stopRatio):
    #check if the remaining value is too small
    remainingValue["stopValue"] = bestVersionRate*stopRatio
    if remainingValue.get("remainingValue") <= bestVersionRate*stopRatio:
        remainingValue["recommendedAction"] = "stop"
    else:
        remainingValue["recommendedAction"] = "continue"
    return(remainingValue)

#define the function to get current time
def gettesttime():
    testTime = datetime.utcnow()
    return(testTime)

#define the function to get current date
def gettestdate(testTime):
    testDate = str(testTime.month) + '/' + str(testTime.day) + '/' + str(testTime.year)
    return(testDate)

#set variables
minValidResponse = 10
percentile = 0.95
stopRatio = 0.01
n = 100000

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

#load the sent email list from cassandra
print "retrieving the sent email list from cassandra"
rawEmailList = session.execute("""
select * from "sentEmailList"
""")

#convert paged results to a list then a dataframe
sentEmailList = pd.DataFrame(list(rawEmailList))

#load the version info from cassandra
print "retrieving the version info data from cassandra"
rawVersionList = session.execute("""
select * from "rawVersionInfo"
""")

#convert paged results to a list then a DataFrame
versionInfo = pd.DataFrame(list(rawVersionList))
#check all inputs are right
checkinputs(versionInfo,sentEmailList,minValidResponse)

#get version test results
versionTestResults = getversiontestresults(sentEmailList,versionInfo)
#print versionTestResults

#get ratios for the next split test
print "getting new version test ratios, where number of iterations = " + str(n) + ", percentile = " + str(percentile) + ", stopRatio = " + str(stopRatio)
print "please wait about 1 minute"
MCResults = getnextsplit(versionTestResults,percentile,stopRatio,n)
nextSplitRatio = MCResults.get("nextSplitRatio")
remainingValue = MCResults.get("remainingValue")
print nextSplitRatio
print remainingValue

#get current date and time
testTime = gettesttime()
testDate = gettestdate(testTime)

#update the version test list
print "updating the version test list into cassandra, please wait about 1 second"
for versionTestData in versionTestResults:
    versionId = versionTestData.get("versionId")
    visits = versionTestData.get("visits")
    clicks = versionTestData.get('clicks')
    rate = versionTestData.get('rate')
    testRatio = nextSplitRatio.get(versionId)
    timeFeature = versionTestData.get('timeFeature')
    titleFeature = versionTestData.get('titleFeature')
    prepared_stmt = session.prepare("""
    INSERT INTO "versionTests" ("versionId","testDate",clicks,rate,"testRatio","testTime","timeFeature","titleFeature",visits)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    bound_stmt = prepared_stmt.bind([versionId,testDate,clicks,rate,testRatio,testTime,timeFeature,titleFeature,visits])
    stmt = session.execute(bound_stmt)
