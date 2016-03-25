#this script creats a MongoDB collection 'fortune500' using the company
#data from the local csv file: 'Fortune500.csv'


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#load company data
def loadcompany():
    print "loading the fortune 500 companies"
    data = []
    with open('Fortune500.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row)
    return(data)

#insert data into MongoDB
def insertdb(data):
    db1.fortune500.insert({"_id":str(ObjectId()),"fortune500":data[0]})
    print "done"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    #load company data
    data = loadcompany()
    #insert to mongodb
    insertdb(data)
