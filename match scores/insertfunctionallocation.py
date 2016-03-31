#this script creats a MongoDB collection 'functionalLocation' using the location
#data from the local csv file: 'FunctionalLocation.csv'


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#load location data
def loadlocation():
    print "loading the functional location info"
    data = []
    with open('FunctionalLocation.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    return(data)

#insert data into MongoDB
def insertdb(data):
    for i,row in enumerate(data):
        row = data[i]
        #clean the empty strings attached
        if '' in row:
            #get the first place we have the empty string in that row
            #and take it as the end position of the location list
            k = row.index('')
        else:
            k = len(row)
        db1.functionalLocation.insert({"_id":str(ObjectId()),"locationName":row[0],"locationList":row[1:k]})
    print str(i+1) + " rows of data have been inserted"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    #load location data
    data = loadlocation()
    #insert into mongodb
    insertdb(data)
