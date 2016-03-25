#this script inserts sales data with date range from 2013-Jan to 2015-Aug
#to cassandra database as "rawSalesData"
#this script must be executed only once
#note that the initial data source is in the format of CSV given external,
#so all the column names and variable names (total_sales) are different from all
#tables (totalSales) as internal sources in cassandra

#important:
#1: the table "rawSalesData" works as a reference, please do not touch it again once created


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd
import csv


#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

#load raw sales data locally
print "loading raw sales data"
data = []
with open('raw_sales_data.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#load into a dataframe
sales = pd.DataFrame(data[1:])
#assign column names
sales.columns = data[0]
#change data type
sales[["order_id","quantity_count","order_count"]] = sales[["order_id","quantity_count","order_count"]].astype(int)
sales[["price","total_sales"]]=sales[["price","total_sales"]].astype(float)

#create the table for raw sales data containing all info, which won't be touched again
session.execute("""
CREATE TABLE IF NOT EXISTS "rawSalesData" (
    "orderId" int,
    "productType" varchar,
    "vendorName" varchar,
    "productTitle" varchar,
    "variantTitle" varchar,
    price float,
    name varchar,
    email varchar,
    "shippingCountry" varchar,
    "shippingProvince" varchar,
    "shippingCity" varchar,
    month varchar,
    day varchar,
    "quantityCount" int,
    "totalSales" float,
    "orderCount" int,
    PRIMARY KEY ("orderId")
)
""")

#insert raw data to cassandra table "rawSalesData"
print "inserting raw sales data to cassandra, please wait about 5 minutes"
n = sales.shape[0]
for i in range(n):
    values = sales.iloc[i].values.tolist()
    prepared_stmt = session.prepare("""
    INSERT INTO "rawSalesData" ("orderId","productType","vendorName","productTitle","variantTitle",price,name,email,"shippingCountry","shippingProvince","shippingCity",month,day,"quantityCount","totalSales","orderCount")
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    bound_stmt = prepared_stmt.bind(values)
    stmt = session.execute(bound_stmt)
print str(n) + " rows of sales data have been inserted"
