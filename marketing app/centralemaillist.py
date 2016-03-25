#this script creates a central email list containing all emails with user-side
#information, the "status" column helps to control the split test process
#this script must be executed only once, but should be updated by setting new 'ranks'

#note that the column 'status' is the indicator for generating each propose email list,
#'sent': this email has been already sent, and it can be found in the table 'sentEmailList'
#'sending': this email will be sent for the next campaign, and it can be found in the table 'finalEmailList'
#'pending': this email hasn't been sent yet, but it will still be considered
#'rejected': this email has been rejected by the administrator, and it will not be considered

#important:
#1: the "name" column has been cleaned against those special characters, so
#names may differ from the raw sales data and only use email as a reference
#2: the 'totalSales', 'quantityCount' and 'orderCount' columns exclude sales from subscription and gift cards

#to do:
#1: add user preferences info


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import datetime
import pandas as pd
import numpy as np
import random
import csv


#define the function to clean the initial email list
def cleanemaillist(initialEmailList):
    #make a copy of raw data
    emailList = initialEmailList.copy()
    #change data type of userId from string to int
    emailList["userId"] = emailList["userId"].astype(int)
    #change data type of missing values to NaN
    emailList.loc[emailList.shippingCountry == '#N/A',"shippingCountry"] = None
    emailList.loc[emailList.shippingProvince == '#N/A',"shippingProvince"] = None
    emailList.loc[emailList.shippingCity == '#N/A',"shippingCity"] = None
    return(emailList)

#define the function to clean sales data
def cleansalesdata(sales, row, value):
    #make a copy of raw sales data
    salesData = sales.copy()
    #change data types
    salesData[["orderId","quantityCount","orderCount"]] = salesData[["orderId","quantityCount","orderCount"]].astype(int)
    salesData[["price","totalSales"]] = salesData[["price","totalSales"]].astype(float)
    #exclude missing values
    salesData = salesData.query('productTitle != "" and name != "" and email != "" and totalSales >= 0')
    #exclude non-beers sales data
    salesData = salesData.query('productType != "Gift Card" and productType != "Subscription" and productType != ""')
    #subset data only for product title, region and the Y variable we want, which is sales
    salesData = salesData[[row,value]]
    #group data by product and total sales
    salesData = pd.pivot_table(salesData,index = [row],values = [value],aggfunc = np.sum,fill_value = 0)
    #reset the row index and turn email from index to a new column
    salesData.reset_index(level=0,inplace=True)
    #print salesData.shape
    #print salesData
    return(salesData)

#define the function to get the last order date per email
def getlastorderdate(sales,row):
    #make a copy of raw sales data
    salesData = sales.copy()
    #clean data
    salesData = salesData.query('productTitle != "" and name != "" and email != "" and productType != ""')
    #convert string to date for comparison
    newDateList = []
    for date in salesData['day']:
        newDate = datetime.datetime.strptime(date,"%m/%d/%y")
        newDateList.append(newDate)
    #print newDateList
    #print salesData.head(10)
    salesData['date'] = newDateList
    #salesData.loc[:,'date'] = pd.Series(newDateList,index = salesData.index)
    #get the last order date
    salesData = pd.pivot_table(salesData,index = [row],values = ['date'],aggfunc = max)
    #convert date to string
    lastOrderDateList = []
    for date in salesData['date']:
        newDate = date.strftime("%m/%d/%y")
        lastOrderDateList.append(newDate)
    #print lastOrderDateList
    salesData["lastOrderDate"] = lastOrderDateList
    #reset the row index and turn email from index to a new column
    salesData.reset_index(level=0,inplace=True)
    #check missing values
    if salesData.isnull().any().any() == True:
        raise ValueError("we have some missing values, which is not expected")
    else:
        #drop the date column which is useless
        salesData = salesData.drop('date', 1)
        return(salesData)

#define the function to get order ids for each user
def getorderids(emailList,sales):
    orderIdsList = []
    for email in emailList['email']:
        orders = sales.loc[sales.email == email,"orderId"].tolist()
        orderIdsList.append(orders)
    emailList["orderIds"] = orderIdsList
    return(emailList)

#define the function to add features to the initial email list
def addfeatures(emailList,featureData,method = 'left'):
    newEmailList = pd.merge(emailList,featureData,on="email",how=method,copy=True)
    return(newEmailList)

#define the function to convert NaN to 0
def convertNaN(emailList):
    #convert NaN in column 'totalSales', 'quantityCount', 'orderCount', 'lastOrderDate' to 0
    emailList.loc[np.isnan(emailList['totalSales']),"totalSales"] = 0
    emailList.loc[np.isnan(emailList['quantityCount']),"quantityCount"] = 0
    emailList.loc[np.isnan(emailList['orderCount']),"orderCount"] = 0
    return(emailList)

#define the function to get initial rank by sorting
def sortbycolumn(emailList,column):
    emailList = emailList.sort_index(by=[column],ascending=[False])
    return(emailList)

#define the function to get current time
def gettesttime():
    testTime = datetime.datetime.utcnow()
    return(testTime)

#define the function to get current date
def gettestdate(testTime):
    testDate = str(testTime.month) + '/' + str(testTime.day) + '/' + str(testTime.year)
    return(testDate)

#define the function to get recommended beers
def getbeers(proposeEmailList,salesData,beersData,k,p):
    #make a copy
    emailList = proposeEmailList.copy()
    #filter out all non-beers sales
    salesData = salesData[~salesData["productType"].isin(["Gift Card","Subscription",""])]
    #get 100 beers listed order by sales
    topBeers = beersData.sort_index(by=['totalSales'],ascending=[False])
    topBeers = topBeers["productTitle"].iloc[0:100].tolist()
    #get recommended beers
    userList = emailList["name"].tolist()
    recommendedBeersList = []
    beersTypeList = []
    for user in userList:
        #get beers bought by this user with total sales
        beers = salesData.loc[salesData.name == user,["productTitle","totalSales"]]
        beers = beers.groupby('productTitle',as_index=False).sum()
        #get beer id and keep all beers which has a valid beer id
        beerIds = []
        for beer in beers["productTitle"].tolist():
            beerId = beersData.loc[beersData.productTitle == beer,"beerId"]
            if beerId.empty == False:
                beerIds.append(beerId.values[0])
            else:
                beerIds.append("None")
        beers["beerId"] = beerIds
        if not beers.empty:
            beers = beers[beers["beerId"] != "None"]
        #then we need to consider several different cases
        #if there are more than k beers, we get top k given by total sales
        if beers.shape[0] >= k:
            beers = beers.sort_index(by=['totalSales'],ascending=[False])
            recommendedBeers = beers["beerId"].iloc[0:k].tolist()
            beersType = "regular"
        #if there are fewer than k beers and more than 1, we randomly choose to add similar beers or top sold beers
        else:
            if random.random() > p and beers.shape[0] > 1:
                #add similar beers (6 similar beers at most)
                similarBeerIds = []
                for beerId in beers["beerId"].tolist()[0:2]:
                    similarBeers = beersData.loc[beersData.productTitle == beer,"similarBeers"]
                    #similarBeers = db1.beers.find_one({"_id":beerId})
                    if similarBeers.empty == False:
                        similarBeerIds += similarBeers.values[0]
                #check whether we get enough beers
                i = 0
                moreBeerIds = []
                while len(set(beers["beerId"].tolist()+similarBeerIds+moreBeerIds)) < k:
                    #if not, add beers from the top sales list
                    #moreBeer = db1.beers.find_one({"overview.name":topBeers[i]})
                    moreBeer = beersData.loc[beersData.productTitle == topBeers[i],"beerId"]
                    i += 1
                    if moreBeer.empty == False:
                        moreBeerIds.append(moreBeer.values[0])
                recommendedBeers = list(set(beers["beerId"].tolist()+similarBeerIds+moreBeerIds))[0:k]
                beersType = "similar"
            else:
                #add beers from the top sales list
                i = 0
                moreBeerIds = []
                while len(set(beers["beerId"].tolist() + moreBeerIds)) < k:
                    #moreBeer = db1.beers.find_one({"overview.name":topBeers[i]})
                    moreBeer = beersData.loc[beersData.productTitle == topBeers[i],"beerId"]
                    i += 1
                    if moreBeer.empty == False:
                        moreBeerIds.append(moreBeer.values[0])
                recommendedBeers = list(set(beers["beerId"].tolist() + moreBeerIds))[0:k]
                beersType = "popular"
        #print beers
        recommendedBeersList.append(recommendedBeers)
        beersTypeList.append(beersType)
    return({"recommendedBeers":recommendedBeersList,"beersType":beersTypeList})

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

#load the initial email list locally
print "loading and cleaning the initial email list"
data = []
with open('initial_email_list.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#load into a dataframe
initialEmailList = pd.DataFrame(data[1:])
#assign column names
initialEmailList.columns = data[0]
#clean data
initialEmailList = cleanemaillist(initialEmailList)

#load raw sales data from cassandra
print "retrieving raw sales data from cassandra"
#rawSales = session.execute("select * from rawSalesData limit 200")
rawSales = session.execute("""
select * from "rawSalesData"
""")
#convert paged results to a list then a dataframe
sales = pd.DataFrame(list(rawSales))

#load similar beers from cassandra
print "retrieving similar beers from cassandra"
beersData = session.execute("""
select * from "beersData"
""")
#convert paged results to a list then a dataframe
# print similarBeers
# aaa
beersData = pd.DataFrame(list(beersData))

#clean data and create the full email list
print "cleaning sales data, and merging to the main email list, please wait about 5 minutes"
#get total sales per email
salesPerEmail = cleansalesdata(sales, row = "email", value = "totalSales")
#get total quantity count per email
quantitiesPerEmail = cleansalesdata(sales, row = "email", value = "quantityCount")
#get total order count per email
ordersPerEmail = cleansalesdata(sales, row = "email", value = "orderCount")
#get the last order date per email
lastOrderDataPerEmail = getlastorderdate(sales,row = "email")

#merge features to the initial email list
mainEmailList = addfeatures(addfeatures(addfeatures(addfeatures(initialEmailList,salesPerEmail),quantitiesPerEmail),ordersPerEmail),lastOrderDataPerEmail,method="inner")
#clean missing values
mainEmailList = convertNaN(mainEmailList)
#get all order ids per email
mainEmailList = getorderids(mainEmailList,sales)
#sort by sales
mainEmailList = sortbycolumn(mainEmailList,column = "totalSales")

#get current date and time
updateTime = gettesttime()
updateDate = gettestdate(updateTime)
#generate recommended beers
recommendedBeers = getbeers(mainEmailList,sales,beersData,k=10,p=0.5)
beersType = recommendedBeers.get("beersType")
recommendedBeers = recommendedBeers.get("recommendedBeers")
# print beersType[1:10]
# print len(beersType)
# print recommendedBeers[1:10]
# print len(recommendedBeers)

#create the table for the main email list containing all users
session.execute("""
CREATE TABLE IF NOT EXISTS "centralEmailList" (
    "userId" int,
    email varchar,
    name varchar,
    "shippingCountry" varchar,
    "shippingProvince" varchar,
    "shippingCity" varchar,
    type varchar,
    "totalSales" float,
    "quantityCount" int,
    "orderCount" int,
    "lastOrderDate" varchar,
    orders set<int>,
    preferences set<varchar>,
    gender varchar,
    age int,
    rank int,
    status varchar,
    "recommendedBeers" list<varchar>,
    "beersType" varchar,
    "updateTime" timestamp,
    "updateDate" varchar,
    PRIMARY KEY (email)
)
""")

#insert raw data to cassandra table "rawSalesData"
print "inserting emails to cassandra, please wait about 5 minutes"
n = mainEmailList.shape[0]
for i in range(n):
    values = mainEmailList.iloc[i].values.tolist()+[None,None,None,i+1,'pending',recommendedBeers[i],beersType[i],updateTime,updateDate]
    #print values
    prepared_stmt = session.prepare("""
    INSERT INTO "centralEmailList" ("userId",email,name,"shippingCountry","shippingProvince","shippingCity",type,"totalSales","quantityCount","orderCount","lastOrderDate",orders,preferences,gender,age,rank,status,"recommendedBeers","beersType","updateTime","updateDate")
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    bound_stmt = prepared_stmt.bind(values)
    stmt = session.execute(bound_stmt)
    print str(i+1) + " emails have been successfully inserted"

#create indices for status and rank
session.execute("""
create index if not exists "centralEmailList_status" on "centralEmailList"(status)
""")
session.execute("""
create index if not exists "centralEmailList_type" on "centralEmailList"(type)
""")
session.execute("""
create index if not exists "centralEmailList_rank" on "centralEmailList"(rank)
""")

#print all info about null values
print "\n the missing value counts are shown as below:"
print mainEmailList.isnull().sum()
# print salesPerEmail.shape
# print quantitiesPerEmail.shape
# print ordersPerEmail.shape
# print lastOrderDataPerEmail.shape
