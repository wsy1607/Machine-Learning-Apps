#this script works for multi-armed bandit test, which is a extended version of A/B test
#
#the basic process is shown as below:
#first get conversion rates for all split versions for one test
#then after applying Monte Carlo simulation for n = 10000 as default, get new traffic
#proportions for the next split test, which are just the baysian probabilities
#finally calculate the remaining value for the splitting test to see whether we should
#terminate the process when one version is significantly better than others
#
#since the real baysian probabilities are too complex, we apply Monte Carlo simulation
#to make approximations. We use the beta distribution generator, Beta(k,n+1-k),
#where k = # of clicks (target metrics), n = # of total visitors (total populations)


#Load packages
import pandas as pd
import numpy as np
from numpy import random
from pymongo import MongoClient


#define the function to simulate convert rates for multiple versions
def getconvertion(k = 3, unit = 50):
    versionList = []
    for i in range(k):
        versionDict = {}
        #get random visitors & random clicks
        n = random.random_integers(1,5)*unit
        versionDict["versionId"] = "version" + str(i)
        versionDict["visits"] = n
        versionDict["clicks"] = int(round(n*random.random_sample(),0))/5
        versionList.append(versionDict)
    return(versionList)

#define the function to get proportions of next split test
def getnextsplit(versionTestResults,percentile,stopRatio,n=10000):
    #collect current convertion rates and MC results
    print versionTestResults
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
    MCData = pd.DataFrame(MCDict)
    #print ratioList
    #get version id
    versionIds = list(MCData.columns.values)
    #get the of the best version
    bestVersionIndex = ratioList.index(max(ratioList))
    #convert simulation results from version id to index
    #for
    #MCData["max"] = versionIds.index(MCData.idxmax(axis = 1))
    maxVersionIndex = []
    for maxVersionId in list(MCData.idxmax(axis = 1)):
        maxVersionIndex.append(versionIds.index(maxVersionId))
    MCData["max"] = maxVersionIndex
    #print MCData
    #print MCData
    #print versionIds
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
    #print stopInfo
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
        #print remainingValueList
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

#set variables
percentile = 0.95
stopRatio = 0.01
n = 100

#connect to the Mongodb
print "connecting to the mongodb from the local server"
client = MongoClient('localhost', 3001)
db = client.meteor

#get split test data
print "getting version test results from mongodb"
versionData = getconvertion(k = 3, unit = 50)
print str(len(versionData)) + " versions are loaded"
print "version info is shown below:"
print versionData

#get ratios for the next split test
print "getting new version test proportions, where number of iterations = " + str(n) + ", percentile = " + str(percentile) + ", stopRatio = " + str(stopRatio)
MCResults = getnextsplit(versionData,percentile,stopRatio,n)
nextSplitRatio = MCResults.get("nextSplitRatio")
remainingValue = MCResults.get("remainingValue")

print nextSplitRatio
print remainingValue
