#this script creats a MongoDB collection 'locationReference' using the location
#data from the local csv file: 'location_list.csv'


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#load data
def loadlocation():
    print "loading location info"
    data = []
    with open('location_list.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    return(data)

#insert data
def insertdb(data):
    for i,row in enumerate(data):
        row = data[i]
        #clean the empty strings attached
        if '' in row:
            #get the first place we have the empty string in that row
            #it is the end position of the location list
            k = row.index('')
        else:
            k = len(row)
        db1.locationReference.insert({"_id":str(ObjectId()),row[0]:row[1:k]})
        print str(i) + " row of data has been inserted"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    #load location data
    locationData = loadlocation()
    #insert into mongodb
    insertdb(locationData)
