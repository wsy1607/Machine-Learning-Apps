#this script creates the propose email list for the next email campaign
#this script should be executed periodically when starting a email campaign

#note that when generating the propose emails, the default "check"
#status is "yes". If we don't want to consider this email forever, turn it
#to "no". If we don't want to consider this email for the next campaign, turn
#it to "pending"

#important:
#1: all filters should be applied when retrieving data from cassandra


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

# #define the function to get recommended beers
# def getbeers(proposeEmailList,salesData,k):
#     #make a copy
#     emailList = proposeEmailList.copy()
#     #filter out all non-beers sales
#     salesData = salesData[~salesData["productType"].isin(["Gift Card","Subscription",""])]
#     #get 100 beers listed order by sales
#     topBeers = salesData[["productTitle","totalSales"]]
#     topBeers = topBeers.groupby('productTitle',as_index=False).sum()
#     topBeers = topBeers.sort_index(by=['totalSales'],ascending=[False])
#     topBeers = topBeers["productTitle"].iloc[0:100].tolist()
#     #get recommended beers
#     userList = emailList["name"].tolist()
#     recommendedBeersList = []
#     beersTypeList = []
#     for user in userList:
#         #get beers bought by this user with total sales
#         beers = salesData.loc[salesData.name == user,["productTitle","totalSales"]]
#         beers = beers.groupby('productTitle',as_index=False).sum()
#         #get beer id and keep all beers which has a valid beer id
#         beerIds = []
#         for beer in beers["productTitle"].tolist():
#             beerId = db1.beers.find_one({"overview.name":beer})
#             if beerId != None:
#                 beerIds.append(beerId.get("_id"))
#             else:
#                 beerIds.append("None")
#         beers["beerId"] = beerIds
#         if not beers.empty:
#             beers = beers[beers["beerId"] != "None"]
#         #then we need to consider several different cases
#         #if there are more than k beers, we get top k given by total sales
#         if beers.shape[0] >= k:
#             beers = beers.sort_index(by=['totalSales'],ascending=[False])
#             recommendedBeers = beers["beerId"].iloc[0:k].tolist()
#             beersType = "regular"
#         #if there are fewer than k beers and more than 1, we randomly choose to add similar beers or top sold beers
#         else:
#             if random.random() > 0.5 and beers.shape[0] > 1:
#                 #add similar beers (6 similar beers at most)
#                 similarBeerIds = []
#                 for beerId in beers["beerId"].tolist()[0:2]:
#                     similarBeers = db1.beers.find_one({"_id":beerId})
#                     if similarBeers != None:
#                         similarBeerIds += similarBeers.get("similarBeers")
#                 #check whether we get enough beers
#                 i = 0
#                 moreBeerIds = []
#                 while len(set(beers["beerId"].tolist()+similarBeerIds+moreBeerIds)) < k:
#                     #if not, add beers from the top sales list
#                     moreBeer = db1.beers.find_one({"overview.name":topBeers[i]})
#                     i += 1
#                     if moreBeer != None:
#                         moreBeerIds.append(moreBeer.get("_id"))
#                 recommendedBeers = list(set(beers["beerId"].tolist()+similarBeerIds+moreBeerIds))[0:k]
#                 beersType = "similar"
#             else:
#                 #add beers from the top sales list
#                 i = 0
#                 moreBeerIds = []
#                 while len(set(beers["beerId"].tolist() + moreBeerIds)) < k:
#                     moreBeer = db1.beers.find_one({"overview.name":topBeers[i]})
#                     i += 1
#                     if moreBeer != None:
#                         moreBeerIds.append(moreBeer.get("_id"))
#                 recommendedBeers = list(set(beers["beerId"].tolist() + moreBeerIds))[0:k]
#                 beersType = "popular"
#         #print beers
#         recommendedBeersList.append(recommendedBeers)
#         beersTypeList.append(beersType)
#     return({"recommenedBeers":recommendedBeersList,"beersType":beersTypeList})
#

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

#define how many emails we would like to check before sending
n = 200

#load the central email list from cassandra, only considering email in "pending" status
print "retrieving the central email list & seles records from cassandra"
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
