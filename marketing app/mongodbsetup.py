#this script migrates the beers collection from mongoDB to Cassandra


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#define the function to get data from mongoDB
def getdata():
    beerIds = []
    beerNames = []
    similarBeers = []
    for beer in db1.beers.find({}):
        beerIds.append(beer.get("_id"))
        beerNames.append(beer.get("overview").get("name"))
        similarBeers.append(beer.get("similarBeers"))
    beers = {}
    beers["beerId"] = beerIds
    beers["productTitle"] = beerNames
    beers["similarBeers"] = similarBeers
    beers = pd.DataFrame(beers)
    return(beers)

#define the function to merge sales data
def addsales(beers,salesData):
    #only for beers
    salesData = salesData[~salesData["productType"].isin(["Gift Card","Subscription",""])]
    #get total sales and quantity count
    bySales = salesData[["productTitle","totalSales"]]
    totalSales = bySales.groupby('productTitle',as_index=False).sum()
    byQuantity = salesData[["productTitle","quantityCount"]]
    quantityCount = byQuantity.groupby('productTitle',as_index=False).sum()
    #define the function for joining two sources to one, the first data source is the main one
    beers = pd.merge(beers,totalSales,on='productTitle',how='left',copy=True)
    beers = pd.merge(beers,quantityCount,on='productTitle',how='left',copy=True)
    beers = beers.fillna(0)
    return(beers)


#connect to mongodb
print "connecting to mongodb from the local server"
client1 = MongoClient('localhost', 3001)
db1 = client1.meteor
# client1 = MongoClient('localhost', 27017)
# db1 = client1.appDB

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

#load raw sales data from cassandra
print "retrieving raw sales data from cassandra"
rawSales = session.execute("""
select * from "rawSalesData"
""")
#convert paged results to a list then a dataframe
sales = pd.DataFrame(list(rawSales))

beersData = getdata()
beersData = addsales(beersData,sales)

#create the table for raw similar beers data
session.execute("""
CREATE TABLE IF NOT EXISTS "beersData" (
    "beerId" varchar,
    "productTitle" varchar,
    "rank" int,
    "similarBeers" list<varchar>,
    "totalSales" float,
    "quantityCount" int,
    PRIMARY KEY ("beerId")
)
""")

#insert raw data to cassandra table "similarBeers"
print "inserting raw sales data to cassandra, please wait about 1 minute"
n = beersData.shape[0]
for i in range(n):
    values = beersData.iloc[i].values.tolist() + [i+1]
    prepared_stmt = session.prepare("""
    INSERT INTO "beersData" ("beerId","productTitle","similarBeers","totalSales","quantityCount","rank")
    VALUES (?, ?, ?, ?, ?, ?)
    """)
    bound_stmt = prepared_stmt.bind(values)
    stmt = session.execute(bound_stmt)
print str(n+1) + " rows of beers data have been inserted"
