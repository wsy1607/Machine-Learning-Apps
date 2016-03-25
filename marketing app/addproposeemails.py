#this script adds the propose email list for the next email campaign
#this script should be executed when we don't have enough pending emails in the propose email list

#note that when adding the propose emails, the default "check"
#status is "yes". If we don't want to consider this email forever, turn it
#to "no". If we don't want to consider this email for the next campaign, turn
#it to "pending"


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
session = cluster.connect('marketingApp')
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
        INSERT INTO "proposeEmailList" (age,"beersType",email,gender,"lastOrderDate",name,"orderCount",orders,preferences,"quantityCount",rank,"recommendedBeers","shippingCity","shippingCountry","shippingProvince",status,"totalSales",type,"updateDate","updateTime","userId",check)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        bound_stmt = prepared_stmt.bind(values)
        stmt = session.execute(bound_stmt)
        counter += 1
    if counter == n:
        print str(n) + " emails have been successfully inserted"
        break
