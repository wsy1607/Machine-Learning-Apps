#this script inserts the local shopify sales data into mongodb


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#connect to mongodb
print "connecting to mongodb from the local server"
client1 = MongoClient('localhost', 3001)
db1 = client1.meteor
# client1 = MongoClient('localhost', 27017)
# db1 = client1.appDB



#load raw sales data locally
print "loading raw sales data"
data = []
with open('raw_sales_data.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#exclude the first row which is the header
sales = data[1:]

print "inserting sales data to mongodb, please wait about 1 minute"
for i,row in enumerate(sales):
    db1.rawSalesData.insert({"_id":str(ObjectId()),"productType":row[0],"vendor":row[1],"productTitle":row[2],"name":row[3],"price":row[4],"shippingProvince":row[5],"variantTitle":row[6],"sku":row[7],"email":row[8],"shippingCity":row[9],"trafficSource":row[10],"quantityCount":row[11],"totalSales":row[12],"region":row[13]})
    print str(i+1) + " rows of data have been inserted"
