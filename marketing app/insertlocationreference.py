#this script inserts the location reference data into mongodb


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#connect to mongodb
print "connecting to mongodb from the local server"
# client1 = MongoClient('localhost', 3001)
# db1 = client1.meteor
client1 = MongoClient('localhost', 27017)
db1 = client1.appDB

print "loading sales info and beer box use info"
data = []
with open('location_list.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

for i,row in enumerate(data):
    row = data[i]
    #clean the empty strings attached
    if '' in row:
        #get the first place we have the empty string in that row
        k = row.index('')
    else:
        k = len(row)
    db1.locationReference.insert({"_id":str(ObjectId()),"locationName":row[0],"locationList":row[1:k]})
    print str(i) + " rows of data have been inserted"
