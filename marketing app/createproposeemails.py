#this script creates the propose email list for the next email campaign
#this script should be executed periodically when starting a email campaign

#note that when generating the propose emails, the default "check"
#status is "yes". If we don't want to consider this email forever, turn it
#to "no". If we don't want to consider this email for the next campaign, turn
#it to "pending"


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime
import pandas as pd
import random
import csv


#define the function to check all inputs
def checkinputs(n,emailList):
    if n > emailList.shape[0]:
        raise ValueError("we don't have enough emails available")
    print "we will generate " + str(n) + " propose emails out of " + str(emailList.shape[0]) + " available emails"

#define the function to sort all emails by rank
def sortbyrank(proposeEmailList):
    #make a copy
    emailList = proposeEmailList.copy()
    #filter by status
    emailList = emailList.loc[emailList.status != 'rejected']
    emailList = emailList.loc[emailList.status != 'sent']
    #sort by rank
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

#load emails
def loademails():
    #load the central email list from cassandra, only considering email in "pending" status
    print "retrieving the central email list with seles records from cassandra"
    rawEmailList = session.execute("""
    select * from "centralEmailList"
    """)

    #convert paged results to a list then a dataframe
    print "preparing the propose email list"
    proposeEmailList = pd.DataFrame(list(rawEmailList))
    return(proposeEmailList)

#insert into cassandra
def insertdb(proposeEmailList,n):
    #create the table for the propose email list
    session.execute("""
    CREATE TABLE IF NOT EXISTS "proposeEmailList" (
        age int,
        "beersType" varchar,
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
        "check" varchar,
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
        INSERT INTO "proposeEmailList" (age,"beersType",email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId",check)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        bound_stmt = prepared_stmt.bind(values)
        stmt = session.execute(bound_stmt)
    print str(i+1) + " emails have been successfully inserted"

    #create indices for status, check and type
    session.execute("""
    create index if not exists "proposeEmailList_status" on "proposeEmailList"(status)
    """)
    session.execute("""
    create index if not exists "proposeEmailList_check" on "proposeEmailList"(check)
    """)
    session.execute("""
    create index if not exists "proposeEmailList_type" on "proposeEmailList"(type)
    """)

#main function
if __name__ == '__main__':
    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect('marketingApp')
    session.row_factory = dict_factory

    #we would like to have 200 propose emails for each round for now
    n = 200
    #get the entire email list
    proposeEmailList = loademails()
    #check all inputs are right
    checkinputs(n,proposeEmailList)
    #create a manual checking column called 'check' for each email
    proposeEmailList = getcheck(sortbyrank(proposeEmailList))
    #get current date and time
    createTime = gettesttime()
    createDate = gettestdate(createTime)
    #insert into cassandra
    insertdb(proposeEmailList,n)
