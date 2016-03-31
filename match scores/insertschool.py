#this script creats a MongoDB collection 'topschool' using the school
#data from the local csv file: 'TopSchoolList.csv'


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#load school data
def loadschool():
    print "loading the top 25 US schools"
    data = []
    with open('TopSchoolList.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    return(data)

#insert data into MongoDB
def insertdb(data):
    db1.topSchool.insert({"_id":str(ObjectId()),"topSchool":data[0]})
    print "done"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    #load school data
    data = loadschool()
    #insert into mongodb
    insertdb(data)
