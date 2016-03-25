#this script creats a MongoDB collection 'rawSalesData' using the historical sales
#data from the local csv file: 'all_yearly_beers_sales.csv'


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#load raw sales data locally
def loadrawsales():
    print "loading raw sales data"
    data = []
    with open('all_yearly_beers_sales.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    #exclude the first row which is the header
    sales = data[1:]
    return(sales)

#insert data into MongoDB
def insertdb(sales):
    print "inserting sales data to mongodb"
    for i,row in enumerate(sales):
        db1.rawSalesData.insert({"_id":str(ObjectId()),"productType":row[0],"vendor":row[1],"productTitle":row[2],"name":row[3],"price":row[4],"shippingProvince":row[5],"variantTitle":row[6],"sku":row[7],"email":row[8],"shippingCity":row[9],"trafficSource":row[10],"quantityCount":row[11],"totalSales":row[12],"region":row[13]})
    print str(i+1) + " row of data has been inserted"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    #load sales data
    sales = loadrawsales()
    #insert to mongodb
    insertdb(sales)
