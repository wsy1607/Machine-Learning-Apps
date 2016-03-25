#find complete documentation in the README.md


from __future__ import absolute_import

from emailCampaign.celery import app


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd
from numpy import random


@app.task(name='createproposeemails')
def createproposeemails():
    #load packages
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    from datetime import datetime
    import pandas as pd
    import csv


    #define the function to check all inputs
    def checkinputs(n,emailList):
        print "we will generate " + str(n) + " propose emails out of " + str(emailList.shape[0]) + " available emails"
        if n > emailList.shape[0]:
            raise ValueError("we don't have enough emails available")

    #define the function to sort all emails by rank
    def sortbyrank(proposeEmailList):
        #make a copy
        emailList = proposeEmailList.copy()
        #filter by status
        emailList = emailList.loc[emailList.status != 'rejected']
        emailList = emailList.loc[emailList.status != 'sent']
        #sort by rank
        #emailList = emailList.sort('rank',ascending = True)
        emailList = emailList.sort_index(by=['status','rank'],ascending=[False,True])
        return(emailList)

    #define the function to create a check column for each user
    def getcheck(proposeEmailList):
        #make a copy
        emailList = proposeEmailList.copy()
        #create the "check" column
        check = ["yes"] * emailList.shape[0]
        emailList["check"] = check
        return(emailList)

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
    session = cluster.connect('craftshack')
    session.row_factory = dict_factory

    #define how many emails we would like to check before sending
    n = 200

    #load the central email list from cassandra, only considering email in "pending" status
    print "retrieving the central email list from cassandra"
    # rawEmailList = session.execute("""
    # select * from "centralEmailList" where status = 'pending'
    # """)
    rawEmailList = session.execute("""
    select * from "centralEmailList"
    """)

    #convert paged results to a list then a dataframe
    print "preparing the propose email list"
    proposeEmailList = pd.DataFrame(list(rawEmailList))
    #check all inputs are right
    checkinputs(n,proposeEmailList)
    #create a manual checking column called 'check' for each email (user)
    proposeEmailList = getcheck(sortbyrank(proposeEmailList))
    #get current date and time
    createTime = gettesttime()
    createDate = gettestdate(createTime)

    #create the table for the propose email list
    session.execute("""
    CREATE TABLE IF NOT EXISTS "proposeEmailList" (
        age int,
        email varchar,
        gender varchar,
        "lastOrderDate" varchar,
        name varchar,
        "orderCount" int,
        orders set<int>,
        preferences set<varchar>,
        "quantityCount" int,
        rank int,
        "recommendedBeers" list<varchar>,
        "shippingCity" varchar,
        "shippingCountry" varchar,
        "shippingProvince" varchar,
        status varchar,
        "totalSales" float,
        type varchar,
        "userId" int,
        check varchar,
        "updateTime" timestamp,
        "updateDate" varchar,
        PRIMARY KEY (email)
    )
    """)

    #insert raw data to cassandra table "proposeEmailList"
    print "inserting propose emails into cassandra, please wait about 1 second"
    for i in range(n):
        values = proposeEmailList.iloc[i].values.tolist()
        prepared_stmt = session.prepare("""
        INSERT INTO "proposeEmailList" (age,email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId",check)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        bound_stmt = prepared_stmt.bind(values)
        stmt = session.execute(bound_stmt)
    print str(i+1) + " emails have been successfully inserted"

    #create indices for status, rank and type
    session.execute("""
    create index if not exists "proposeEmailList_status" on "proposeEmailList"(status)
    """)
    session.execute("""
    create index if not exists "proposeEmailList_check" on "proposeEmailList"(check)
    """)
    session.execute("""
    create index if not exists "proposeEmailList_type" on "proposeEmailList"(type)
    """)

@app.task(name='addproposeemails')
def addproposeemails():
    #load packages
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    import pandas as pd
    from numpy import random


    #define the function to check all inputs
    def checkinputs(n,emailList):
        print "we will generate " + str(n) + " more propose emails out of " + str(emailList.shape[0]) + " available emails"
        if n > emailList.shape[0]:
            raise ValueError("we don't have enough emails available")

    #define the function to sort all emails by rank
    def sortbyrank(proposeEmailList):
        #make a copy
        emailList = proposeEmailList.copy()
        #sort by rank
        emailList = emailList.sort('rank',ascending = True)
        return(emailList)

    #define the function to create a check column for each user
    def getcheck(proposeEmailList):
        #make a copy
        emailList = proposeEmailList.copy()
        #create the "check" column with default value "yes"
        check = ["yes"] * emailList.shape[0]
        emailList["check"] = check
        return(emailList)

    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect('craftshack')
    session.row_factory = dict_factory

    #define how many emails we would like to check before sending
    n = 200

    #load the central email list from cassandra, only considering email in "pending" status
    print "retrieving the central email list from cassandra"
    rawEmailList = session.execute("""
    select * from "centralEmailList" where status = 'pending'
    """)

    #convert paged results to a list then a dataframe
    print "preparing the propose email list"
    moreProposeEmails = pd.DataFrame(list(rawEmailList))
    #check all inputs are right
    checkinputs(n,moreProposeEmails)
    #create a manual checking column called 'check' for each email (user)
    moreProposeEmails = sortbyrank(getcheck(moreProposeEmails))

    #load the propose email list from cassandra
    print "retrieving the all propose emails from cassandra"
    rawEmailList = session.execute("""
    select email from "proposeEmailList"
    """)

    proposeEmailList = pd.DataFrame(list(rawEmailList))["email"].tolist()

    #insert raw data to cassandra table "proposeEmailList"
    print "inserting more propose emails into cassandra, please wait about 1 second"
    counter = 0
    for i in range(moreProposeEmails.shape[0]):
        if moreProposeEmails.iloc[i]['email'] not in proposeEmailList:
            values = moreProposeEmails.iloc[i].values.tolist()
            prepared_stmt = session.prepare("""
            INSERT INTO "proposeEmailList" (age,email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId",check)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            bound_stmt = prepared_stmt.bind(values)
            stmt = session.execute(bound_stmt)
            counter += 1
        if counter == n:
            print str(n) + " emails have been successfully inserted"
            break

@app.task(name='createfinalemails')
def createfinalemails():
    #load packages
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    import pandas as pd
    from numpy import random
    import csv


    #define the function to check all inputs
    def checkinputs(n,approvedEmailList,rejectedEmailList):
        print "we will generate " + str(n) + " final emails out of " + str(approvedEmailList.shape[0]) + " approved propose emails"
        if n > approvedEmailList.shape[0]:
            raise ValueError("we don't have enough emails available")

    #define the function to filter the email list by status
    def filterbycheck(proposeEmailList,check):
        #make a copy
        emailList = proposeEmailList.copy()
        #filter by status
        emailList = emailList.loc[emailList.check == check]
        return(emailList)

    #define the function to sort all emails by rank
    def sortbyrank(proposeEmailList):
        #make a copy
        emailList = proposeEmailList.copy()
        #sort by rank
        emailList = emailList.sort('rank',ascending = True)
        return(emailList)

    #define the function to get the test ratio of the last update
    def getversionratio(versionData):
        lastUpdateTime = max(versionData["testTime"])
        versionRatio = versionData.loc[versionData.testTime == lastUpdateTime,["versionId","testRatio"]]
        return(versionRatio)

    #define the function to randomly assign the version to each email user
    def getversionid(finalEmailList,versionRatio):
        testVersionIds = list(versionRatio["versionId"])
        ratios = list(versionRatio["testRatio"])
        #print ratios
        #print 1 - sum(ratios)
        #check
        if len(testVersionIds) != len(ratios):
            raise ValueError("cannot create final emails, because versionIds are not unique")
        if abs(1 - sum(ratios)) > 0.01:
            raise ValueError("cannot create final emails, because the sum of testRatios is " + str(sum(ratios)) + " which is not equal to 1")
        elif abs(1 - sum(ratios)) > 1e-8:
            ratios[-1] = 1 - sum(ratios) + ratios[-1]
        #print versionIds
        emailVersionIds = []
        for i in range(finalEmailList.shape[0]):
            versionId = testVersionIds[random.choice(len(ratios),1,p=ratios)]
            emailVersionIds.append(versionId)
        finalEmailList["versionId"] = emailVersionIds
        return(finalEmailList)

    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect('craftshack')
    session.row_factory = dict_factory

    #define how many emails we would like to send
    n = 100

    #load the central email list from cassandra
    print "retrieving the propose email list from cassandra"
    rawEmailList = session.execute("""
    select * from "proposeEmailList"
    """)

    #load the version test data from cassandra
    print "retrieving the version test data from cassandra"
    versionData= session.execute("""
    select * from "versionTests"
    """)

    #convert paged results to a list then a dataframe for version data
    versionData = pd.DataFrame(list(versionData))
    #get the most recent test ratio
    versionRatio = getversionratio(versionData)
    #convert paged results to a list then a dataframe for the email list
    proposeEmailList = pd.DataFrame(list(rawEmailList))
    #check the propose email list
    if proposeEmailList.shape[0] < 1:
        raise ValueError("the propose email list isn't ready, please generate propose emails first")
    #get the approved final email list and the rejected email list
    approvedEmailList = sortbyrank(filterbycheck(proposeEmailList,check = "yes"))
    print str(approvedEmailList.shape[0]) + " approved emails have been loaded"
    rejectedEmailList = filterbycheck(proposeEmailList, check = "no")
    print str(rejectedEmailList.shape[0]) + " rejected emails have been loaded"
    #check all inputs are right
    checkinputs(n,approvedEmailList,rejectedEmailList)

    #create the final email list for the next campaign
    session.execute("""
    CREATE TABLE IF NOT EXISTS "finalEmailList" (
        age int,
        check varchar,
        email varchar,
        gender varchar,
        "lastOrderDate" varchar,
        name varchar,
        "orderCount" int,
        orders set<int>,
        preferences set<varchar>,
        "quantityCount" int,
        rank int,
        "recommendedBeers" list<varchar>,
        "shippingCity" varchar,
        "shippingCountry" varchar,
        "shippingProvince" varchar,
        status varchar,
        "totalSales" float,
        type varchar,
        "userId" int,
        "versionId" varchar,
        "updateTime" timestamp,
        "updateDate" varchar,
        PRIMARY KEY (email)
    )
    """)

    #insert raw data to cassandra table "finalEmailList"
    print "inserting approved final emails into 'finalEmailList', please wait about 1 minute"
    for i in range(n):
        values = approvedEmailList.iloc[i]
        #change the status
        values["status"] = "sending"
        values = values.values.tolist() + [None]
        prepared_stmt = session.prepare("""
        INSERT INTO "finalEmailList" (age,check,email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId","versionId")
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        bound_stmt = prepared_stmt.bind(values)
        stmt = session.execute(bound_stmt)
    print str(i+1) + " emails have been successfully inserted into the final email list"

    #create indices for status, check, type and 'versionId'
    session.execute("""
    create index if not exists "finalEmailList_status" on "finalEmailList"(status)
    """)
    session.execute("""
    create index if not exists "finalEmailList_check" on "finalEmailList"(check)
    """)
    session.execute("""
    create index if not exists "finalEmailList_type" on "finalEmailList"(type)
    """)
    session.execute("""
    create index if not exists "finalEmailList_versionId" on "finalEmailList"("versionId")
    """)

    #update the central email list
    print "updating the central email list in cassandra, please wait about 1 minute"
    for i in range(n):
        status = "sending"
        email = approvedEmailList.iloc[i]['email']
        prepared_stmt = session.prepare ("""
        UPDATE "centralEmailList" SET status = ? WHERE email = ?
        """)
        bound_stmt = prepared_stmt.bind([status,email])
        stmt = session.execute(bound_stmt)
    print str(i+1) + " approved emails have been successfully updated to the central email list"

    for i in range(rejectedEmailList.shape[0]):
        status = "rejected"
        email = rejectedEmailList.iloc[i]['email']
        prepared_stmt = session.prepare ("""
        UPDATE "centralEmailList" SET status = ? WHERE email = ?
        """)
        bound_stmt = prepared_stmt.bind([status,email])
        stmt = session.execute(bound_stmt)
    print str(rejectedEmailList.shape[0]) + " rejected emails have been successfully updated to the central email list"

    #reload the final email list from cassandra
    print "reloading the final email list from cassandra"
    rawEmailList = session.execute("""
    select * from "finalEmailList"
    """)

    #convert paged results to a list then a dataframe
    finalEmailList = pd.DataFrame(list(rawEmailList))

    #assign version id to each email user
    finalEmailList = getversionid(finalEmailList,versionRatio)

    #update version id to cassandra table "finalEmailList"
    print "updating version id to final emails into cassandra, please wait about 1 minute"
    n = finalEmailList.shape[0]
    for i in range(n):
        email = finalEmailList['email'][i]
        versionId = finalEmailList['versionId'][i]
        #print userId
        #print email
        prepared_stmt = session.prepare ("""
        UPDATE "finalEmailList" SET "versionId" = ? WHERE (email = ?)
        """)
        bound_stmt = prepared_stmt.bind([versionId,email])
        stmt = session.execute(bound_stmt)
    print str(n) + " version ids have been updated"

@app.task(name='cleanfinalemails')
def cleanfinalemails():
    #load packages
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    import pandas as pd


    #define the function to check all inputs
    def checkinputs(sentEmailList):
        print "we have sent " + str(sentEmailList.shape[0]) + " emails"
        if sentEmailList.shape[0] < 1:
            raise ValueError("we should send final emails before cleaning the list")

    #define the function to change status from 'sending' to 'sent'
    def changestatus(sentEmailList):
        sentEmailList["status"] = 'sent'
        return(sentEmailList)

    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect('craftshack')
    session.row_factory = dict_factory

    #load the central email list from cassandra
    print "retrieving the final email list from cassandra"
    rawEmailList = session.execute("""
    select * from "finalEmailList"
    """)

    #convert paged results to a list then a dataframe
    sentEmailList = pd.DataFrame(list(rawEmailList))
    #check all inputs are right
    checkinputs(sentEmailList)
    #change status
    sentEmailList = changestatus(sentEmailList)

    #create the final email list for the next campaign
    session.execute("""
    CREATE TABLE IF NOT EXISTS "sentEmailList" (
        age int,
        check varchar,
        email varchar,
        gender varchar,
        "lastOrderDate" varchar,
        name varchar,
        "orderCount" int,
        orders set<int>,
        preferences set<varchar>,
        "quantityCount" int,
        rank int,
        "recommendedBeers" list<varchar>,
        "shippingCity" varchar,
        "shippingCountry" varchar,
        "shippingProvince" varchar,
        status varchar,
        "totalSales" float,
        type varchar,
        "userId" int,
        "versionId" varchar,
        response varchar,
        "updateTime" timestamp,
        "updateDate" varchar,
        PRIMARY KEY (email)
    )
    """)

    #insert raw data to cassandra table "sentEmailList"
    print "inserting all sent emails into cassandra, please wait about 1 minute"
    for i in range(sentEmailList.shape[0]):
        values = sentEmailList.iloc[i].values.tolist() + ["no"]
        prepared_stmt = session.prepare("""
        INSERT INTO "sentEmailList" (age,check,email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId","versionId",response)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        bound_stmt = prepared_stmt.bind(values)
        stmt = session.execute(bound_stmt)
    print str(i+1) + " emails have been successfully inserted into the sent email list"

    #create indices for age, gender, type and response
    session.execute("""
    create index if not exists "sentEmailList_response" on "sentEmailList"(response)
    """)
    session.execute("""
    create index if not exists "sentEmailList_age" on "sentEmailList"(age)
    """)
    session.execute("""
    create index if not exists "sentEmailList_gender" on "sentEmailList"(gender)
    """)
    session.execute("""
    create index if not exists "sentEmailList_type" on "sentEmailList"(type)
    """)

    #update 'status' in table "centralEmailList" to cassandra
    print "updating status to final emails in the central email list, please wait about 1 second"
    n = sentEmailList.shape[0]
    for i in range(n):
        email = sentEmailList['email'][i]
        status = 'sent'
        prepared_stmt = session.prepare ("""
        UPDATE "centralEmailList" SET "status" = ? WHERE email = ?
        """)
        bound_stmt = prepared_stmt.bind([status,email])
        stmt = session.execute(bound_stmt)
    print str(n) + " emails have been updated"

    #clean table "proposeEmailList" and table "finalEmailList"
    session.execute("""
    drop table "proposeEmailList"
    """)
    session.execute("""
    drop table "finalEmailList"
    """)

@app.task(name='updateversionratio')
def updateversionratio():
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
        for versionId in versionIds:
            versionTestResultsDict = {}
            versionVisits = sentEmailList.loc[sentEmailList.versionId == versionId]
            versionClicks = versionVisits.loc[versionVisits.response != 'no']
            versionTestResultsDict["versionId"] = versionId
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
    session = cluster.connect('craftshack')
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
    checkinputs(versionInfo,sentEmailList)

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

@app.task(name='updateemailrank')
def updateemailrank():
    #load packages
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    from datetime import datetime
    import collections
    import pandas as pd
    import numpy as np
    import csv
    from sklearn.ensemble import RandomForestRegressor
    from sklearn import linear_model
    from sklearn import svm


    #define the function to check the feedback (response)
    def checkresponse(sentEmailList,minValidResponse):
        response = sentEmailList["response"].values.tolist()
        responseCounter = collections.Counter(response)
        validResponseCounter = 0
        for responseKey in responseCounter.keys():
            print str(responseKey) + " : " + str(responseCounter[responseKey])
            #calculate for # of valid response
            if responseKey != "no":
                validResponseCounter += responseCounter[responseKey]
        if validResponseCounter < minValidResponse:
            raise ValueError("not enough data, the number of valid responses is too small")

    #define the function to get the response for each sent email
    def getresponserating(sentEmailList,ratingParameters):
        responseRatingList = []
        for response in sentEmailList['response']:
            responseRating = ratingParameters.get(response)
            responseRatingList.append(responseRating)
        sentEmailList["responseRating"] = responseRatingList
        return(sentEmailList)

    #define the function to get the waiting day (# of days from last order)
    def getwaitingday(sentEmailList):
        #get today's date
        dateFormat = "%m/%d/%y"
        today = datetime.now().strftime(dateFormat)
        #print today
        waitingDayList = []
        for lastOrderDate in sentEmailList["lastOrderDate"]:
            dateDiff = datetime.strptime(today,dateFormat) - datetime.strptime(lastOrderDate,dateFormat)
            waitingDayList.append(dateDiff.days)
        sentEmailList["waitingDay"] = waitingDayList
        return(sentEmailList)

    #define the function to get training data
    def gettraining(data,XColumns,dummyColumns,YColumn = [],dropNA = True,shuffle = True):
        #make a copy
        trainingData = data.copy()
        #drop a few columns which we won't use for ML models
        trainingData = trainingData[XColumns+YColumn]
        if dropNA == True:
            #drop Na values if necessary
            trainingData = trainingData.dropna()
        if shuffle == True:
            #shuffle data if necessary
            trainingData = trainingData.reindex(np.random.permutation(trainingData.index))
        print "the training data has " + str(trainingData.shape[0]) + " rows and " + str(trainingData.shape[1]) + " columns"
        #convert categorical data to dummy variables
        if dummyColumns != []:
            trainingData = getdummy(trainingData,dummyColumns,stage = "training")
        return(trainingData)

    #define the function to convert categorical data to dummy variables
    def getdummy(rawData,categories,stage):
        #make a copy
        data = rawData.copy()
        if stage == "training":
            for category in categories:
                columns = list(data.columns.values)
                #print data[category]
                columnValues = set(data[category])
                #print columnValues
                dummy = pd.get_dummies(data[category],prefix=category)
                #print dummy.head(10)
                if dummy.shape[1] > 1:
                    columns.remove(category)
                    data = data[columns].join(dummy.ix[:,1:])
                elif dummy.shape[1] == 1:
                    columns.remove(category)
                    data = data[columns].join(dummy)
        if stage == "testing":
            #print categories
            columns = list(data.columns.values)
            for category in categories:
                columnValues = set(data[category])
                #print columnValues
                dummy = pd.get_dummies(data[category],prefix=category)
                #print dummy.head(10)
                dummyColumns = list(dummy.columns.values)
                for dummyColumn in dummyColumns:
                    if dummyColumn in columns:
                        data[dummyColumn] = dummy[dummyColumn]
                columns.remove(category)
            data = data[columns]
                #print dummy.head(10)
        return(data)

    #define the function to normalize data SVM
    def normalize(data):
        #normalize data by mean and standard deviation
        data_norm = (data - data.mean()) / data.std()
        #if the column is a constant, keep the original value
        ind = [i for i, x in enumerate(data.std()) if x == 0]
        if ind != []:
            data_norm.ix[:,ind] = data.ix[:,ind]
        return(data_norm)

    #define the function to calculate the random forest CV error, k = 5 for default
    def RFCV(data,YColumn,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200):
        #convert YColumn from a list to a string
        YColumn = YColumn[0]
        #make number of rows divisible by 5
        n = data.shape[0]/k*k
        data = data.iloc[range(n)]
        #set up the initial values for these two tuning parameters
        nCandidates = [2,5,20,50,100,200,300,400,500,700,1000]
        numTrees = nCandidates[:nCandidates.index(maxNumTrees)+1]
        depths = range(1,maxDepth+1)
        #first tune depth with initial number of trees
        print "start tuning the max depth"
        depthErrors = []
        for d in depths:
            #begin k-fold CV
            CVtestMSE = 0
            for i in range(k):
                #get training data & test data split
                testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
                trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
                #get test & training & target
                training = trainingData.drop(YColumn,axis=1)
                target = trainingData[YColumn]
                testing = testingData.drop(YColumn,axis=1)
                #get model
                model = RandomForestRegressor(n_estimators=nTreeInitial,max_depth=d,max_features="sqrt")
                model = model.fit(training,target)
                #evaluate model and compute test error
                pred = np.array(model.predict(testing))
                testY = np.array(testingData[YColumn])
                CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
            #append test errors
            depthErrors.append(CVtestMSE)
        #get the best maxDepth
        bestDepth = depths[depthErrors.index(min(depthErrors))]
        #then tune number of trees
        print "start tuning the number of trees"
        nErrors = []
        for numTree in numTrees:
            #begin k-fold CV
            CVtestMSE = 0
            for i in range(k):
                #get training data & test data split
                testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
                trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
                #get test & training & target
                training = trainingData.drop(YColumn,axis=1)
                target = trainingData[YColumn]
                testing = testingData.drop(YColumn,axis=1)
                #get model
                model = RandomForestRegressor(n_estimators=numTree,max_depth=bestDepth,max_features="sqrt")
                model = model.fit(training,target)
                #evaluate model and compute test error
                pred = np.array(model.predict(testing))
                testY = np.array(testingData[YColumn])
                CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
            #append test errors
            nErrors.append(CVtestMSE)
        #get the best numTrees
        bestNumTree = numTrees[nErrors.index(min(nErrors))]
        print 'Test Mean Squared Error for depths:'
        print depthErrors
        print 'Test Mean Squared Error for number of trees:'
        print nErrors
        print 'The final depth is ' + str(bestDepth)  + ' and the final number of trees is ' + str(bestNumTree) + '.'
        return([[bestDepth,bestNumTree],min(nErrors)])

    #define the function to calculate the lasso CV error, k = 5 for default
    def lassoCV(data,YColumn,k = 5,maxAlpha = 10):
        #convert YColumn from a list to a string
        YColumn = YColumn[0]
        #make number of rows divisible by 5
        n = data.shape[0]/k*k
        data = data.iloc[range(n)]
        #set up the initial values for tuning parameter, alpha
        alphaCandidates = [0.1,0.5,1,2,5,10]
        alphas =  alphaCandidates[:alphaCandidates.index(maxAlpha)+1]
        #tune the penalty parameter alpha
        print "start tuning the penalty parameter, alpha"
        lassoErrors = []
        for a in alphas:
            #begin k-fold CV
            CVtestMSE = 0
            for i in range(k):
                #get training data & test data split
                testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
                trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
                #get test & training & target
                training = trainingData.drop(YColumn,axis=1)
                target = trainingData[YColumn]
                testing = testingData.drop(YColumn,axis=1)
                #normalize data
                training = normalize(training)
                testing = normalize(testing)
                #get model
                model = linear_model.Lasso(alpha = a,normalize=True)
                model = model.fit(training,target)
                #evaluate model and compute test error
                pred = np.array(model.predict(testing))
                testY = np.array(testingData[YColumn])
                CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
            #append test errors
            lassoErrors.append(CVtestMSE)
        #get the best alpha
        bestAlpha = alphas[lassoErrors.index(min(lassoErrors))]
        print 'Test Mean Squared Error for alphas:'
        print lassoErrors
        print 'The final alpha is ' + str(bestAlpha) + '.'
        return([bestAlpha,min(lassoErrors)])

    #define the function to calculate the ridge CV error, k = 5 for default
    def ridgeCV(data,YColumn,k = 5,maxAlpha = 10):
        #convert YColumn from a list to a string
        YColumn = YColumn[0]
        #make number of rows divisible by 5
        n = data.shape[0]/k*k
        data = data.iloc[range(n)]
        #set up the initial values for tuning parameter, alpha
        alphaCandidates = [0.1,0.5,1,2,5,10]
        alphas =  alphaCandidates[:alphaCandidates.index(maxAlpha)+1]
        #tune the penalty parameter, alpha
        print "start tuning the penalty parameter, alpha"
        ridgeErrors = []
        for a in alphas:
            #begin k-fold CV
            CVtestMSE = 0
            for i in range(k):
                #get training data & test data split
                testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
                trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
                #get test & training & target
                training = trainingData.drop(YColumn,axis=1)
                target = trainingData[YColumn]
                testing = testingData.drop(YColumn,axis=1)
                #normalize data
                training = normalize(training)
                testing = normalize(testing)
                #get model
                model = linear_model.Ridge(alpha = a,normalize=True)
                model = model.fit(training,target)
                #evaluate model and compute test error
                pred = np.array(model.predict(testing))
                testY = np.array(testingData[YColumn])
                CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
            #append test errors
            ridgeErrors.append(CVtestMSE)
        #get the best alpha
        bestAlpha = alphas[ridgeErrors.index(min(ridgeErrors))]
        print 'Test Mean Squared Error for alphas:'
        print ridgeErrors
        print 'The final alpha is ' + str(bestAlpha) + '.'
        return([bestAlpha,min(ridgeErrors)])

    #define the function to calculate the linear regression CV error, k = 5 for default
    def linearCV(data,YColumn,k = 5):
        #convert YColumn from a list to a string
        YColumn = YColumn[0]
        #make number of rows divisible by 5
        n = data.shape[0]/k*k
        data = data.iloc[range(n)]
        #start the CV process
        CVtestMSE = 0
        for i in range(k):
            #get training data & test data split
            testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
            trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
            #get test & training & target
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            testing = testingData.drop(YColumn,axis=1)
            #get model
            model = linear_model.LinearRegression()
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[YColumn])
            CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
        #get the CV error
        linearError = CVtestMSE
        print 'Test Mean Squared Error for linear regression:'
        print linearError
        return(["",linearError])

    #define the function to fit the best model to all data to get ratings
    def bestfit(allData,trainingData,model,parameter,YColumn):
        #convert YColumn from a list to a string
        YColumn = YColumn[0]
        #make a copy
        data = allData.copy()
        #fit the best model and make prediction
        #the possible model woule be "random forest", "lasso", "ridge", "linear regression"
        if model == "RF":
            #get parameters
            bestDepth = parameter[0]
            numTree = parameter[1]
            #get test & training & target
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            #fit
            model = RandomForestRegressor(n_estimators=numTree,max_depth=bestDepth,max_features="sqrt")
            model = model.fit(training,target)
            #predict
            pred = model.predict(data)
            allData[YColumn] = pred
        elif model == "lasso":
            #get parameters
            a = parameter
            #get test & training & target
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            #normalize data
            training = normalize(training)
            data = normalize(data)
            #get model
            model = linear_model.Lasso(alpha = a,normalize=True)
            model = model.fit(training,target)
            #predict
            pred = model.predict(data)
            allData[YColumn] = pred
        elif model == "ridge":
            #get parameters
            a = parameter
            #get test & training & target
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            #normalize data
            training = normalize(training)
            data = normalize(data)
            #get model
            model = linear_model.Ridge(alpha = a,normalize=True)
            model = model.fit(training,target)
            #predict
            pred = model.predict(data)
            allData[YColumn] = pred
        elif model == "linear":
            #get test & training & target
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            #get model
            model = linear_model.LinearRegression()
            model = model.fit(training,target)
            #predict
            pred = model.predict(data)
            allData[YColumn] = pred
        else:
            raise ValueError("model not found")
        return(allData)

    #define the function to get the updated rank for all emails in the central email list
    def updaterank(centralEmailList,dataOutput,YColumn):
        #data = pd.concat([centralEmailList, dataOutput], axis=1)
        YColumn = YColumn[0]
        #print dataOutput.head(10)
        #update the column 'rating' by predicted values
        #dataOutput = dataOutput.sort_index(by=[YColumn],ascending=[False])
        #newRating = dataOutput[YColumn]
        #join updated rating
        updatedEmailList = pd.concat([centralEmailList, dataOutput[["responseRating"]]], axis=1)
        if updatedEmailList["responseRating"].isnull().any():
            raise ValueError("we have missed some ratings, which is not expected")
        if updatedEmailList.shape[0] != centralEmailList.shape[0]:
            raise ValueError("we have some missing values, which is not expected")
        #print updatedEmailList.head(10)
        updatedEmailList = updatedEmailList.sort([YColumn],ascending=[False])
        #print updatedEmailList.head(5)
        return(updatedEmailList)

    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect('craftshack')
    session.row_factory = dict_factory

    #define the email rating paramters
    ratingParameters = {"no":0,"open":1,"reply":2,"interested":3}
    minValidResponse = 10

    #load the sent email list (the most recent) from cassandra
    print "retrieving the most recent sent email list as training data from cassandra"
    rawEmailList = session.execute("""
    select * from "sentEmailList"
    """)

    #convert paged results to a list then a dataframe
    sentEmailList = pd.DataFrame(list(rawEmailList))
    #pre-check and summarize all responses
    checkresponse(sentEmailList,minValidResponse)

    #calculate the rank for each email (user)
    print "preparing the training data"
    sentEmailList = getresponserating(sentEmailList,ratingParameters)
    sentEmailList = getwaitingday(sentEmailList)

    #load the sent email list (the most recent) from cassandra
    print "retrieving all emails in the central email list as test data from cassandra"
    rawEmailList = session.execute("""
    select * from "centralEmailList"
    """)

    #convert paged results to a list then a dataframe
    centralEmailList = pd.DataFrame(list(rawEmailList))

    #calculate the rank for each email (user)
    print "preparing the test data from the central email list with " + str(centralEmailList.shape[0]) + " emails"
    centralEmailList = getwaitingday(centralEmailList)

    #get shuffled training data, selected a few columns which will be considered
    print "getting training data"
    #we will add more columns later
    #XColumns = ["gender","waitingDay","orderCount","preferences","quantityCount","age","shippingProvince","type"]
    XColumns = ["waitingDay","totalSales","orderCount","quantityCount","type"]
    YColumn = ["responseRating"]
    dummyColumns = ["type"]
    data = gettraining(sentEmailList,XColumns,dummyColumns,YColumn,dropNA = True,shuffle = True)

    #find the best random forest model by cross validation
    print "getting the best random forest model"
    [RFParameters,RFError] = RFCV(data,YColumn,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200)
    #find the best lasso model by cross validation

    print "getting the best lasso model"
    [lassoParameter,lassoError] = lassoCV(data,YColumn,k = 5,maxAlpha = 10)

    #find the best rigde model by cross validation
    print "getting the best ridge model"
    [ridgeParameter,ridgeError] = ridgeCV(data,YColumn,k = 5,maxAlpha = 10)

    #fit the simple linear model by cross validation
    print "fitting the simple linear model"
    [linearParameter,linearError] = linearCV(data,YColumn,k = 5)

    #print the error for each model and get the best model with smallest CV error
    modelTypes = ["RF","lasso","ridge","linear"]
    modelParameters = [RFParameters,lassoParameter,ridgeParameter,linearParameter]
    modelErrors = [RFError,lassoError,ridgeError,linearError]
    bestModel = modelTypes[modelErrors.index(min(modelErrors))]
    bestParameter = modelParameters[modelErrors.index(min(modelErrors))]
    print "the smallest error for each model:"
    print "random forest:"+str(RFError)+", lasso:"+str(lassoError)+", ridge:"+str(ridgeError)+", linear:"+str(linearError)

    #fit the best model to all emails in the central email list
    print "working on final fit using the best selected model, which is " + bestModel
    #for unseen training data, we don't need to have the y variable
    allData = gettraining(centralEmailList,XColumns,dummyColumns,dropNA=False,shuffle=False)
    dataOutput = bestfit(allData,data,model = bestModel,parameter = bestParameter,YColumn=YColumn)
    #update the rank based on the predicted response ratings
    updatedEmailList = updaterank(centralEmailList,dataOutput,YColumn)

    #update all users in the central email list for new ranks
    print "update the rank in 'centralEmailList' to cassandra, please wait about 1 minute"
    n = updatedEmailList.shape[0]
    for i in range(n):
        #rank is based on sorted predicted rating. which is 'responseRating'
        rank = i + 1
        email = updatedEmailList.iloc[i]['email']
        prepared_stmt = session.prepare ("""
        UPDATE "centralEmailList" SET rank = ? WHERE email = ?
        """)
        bound_stmt = prepared_stmt.bind([rank,email])
        stmt = session.execute(bound_stmt)
    print str(n) + " rows of data have been updated"

@app.task(name='updateemailstatus')
def updateemailstatus():
    #load packages
    from cassandra.cluster import Cluster
    from cassandra.query import dict_factory
    import pandas as pd


    #define the function to check all inputs
    def checkinputs(emails,status,centralEmailList):
        for email in emails:
            if email not in centralEmailList["email"].tolist():
                raise ValueError("the input email " + email + " does not exist in the central email list")
        if status not in ["preferred","pending","rejected","sent"]:
            raise ValueError("the input status " + status + " is not valid")

    #input all emails whose statuses will be changed
    emails = ["lionrunner73@yahoo.com","matthew.gates@ge.com"]

    #input the status option which we would like those emails to go to
    status = "preferred"

    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect('craftshack')
    session.row_factory = dict_factory

    #load the sent email list (the most recent) from cassandra
    print "retrieving all emails in the central email list as test data from cassandra"
    rawEmailList = session.execute("""
    select * from "centralEmailList"
    """)

    #convert paged results to a list then a dataframe
    centralEmailList = pd.DataFrame(list(rawEmailList))

    #check all inputs
    checkinputs(emails,status,centralEmailList)

    #update selected users in the central email list for new statuses
    print "update the status in 'centralEmailList' to cassandra, please wait about 1 minute"
    n = len(emails)
    for i in range(n):
        newStatus = status
        email = emails[i]
        prepared_stmt = session.prepare ("""
        UPDATE "centralEmailList" SET status = ? WHERE email = ?
        """)
        bound_stmt = prepared_stmt.bind([newStatus,email])
        stmt = session.execute(bound_stmt)
    print str(n) + " rows of data have been updated"
