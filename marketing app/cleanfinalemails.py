#this script cleans the final email list and append the list to the email sent list
#this script should be executed periodically after sending all emails from the final email list

#note that we will append all sent emails to the sent email list with response = 'no'
#as default. In the mean time, we will update 'centralEmailList' by setting 'sending'
#to 'sent' for those sent emails and drop 'propseEmailList' and 'finalEmailList'

#important:
#1: all filters should be applied when retrieving data from cassandra


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
session = cluster.connect('marketingApp')
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
    INSERT INTO "sentEmailList" (age,"beersType","check",email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId","versionId",response)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
