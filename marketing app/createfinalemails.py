#this script creates the final email list for the next email campaign
#this script should be executed periodically after manually checking
#the propose email list

#note that when generating the final emails, we only consider emails in the
#propose email list with status = 'pending' and check = 'yes'. After inserting
#emails into the final email list, we assign the available versions randomly
#with probability p, where p = the correponding version test ratio

#important:
#1: all filters should be applied when retrieving data from cassandra

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
session = cluster.connect('marketingApp')
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
    "beersType" varchar,
    "check" varchar,
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
    INSERT INTO "finalEmailList" (age,"beersType","check",email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId","versionId")
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
