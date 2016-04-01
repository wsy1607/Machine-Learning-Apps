#this script creats a MongoDB collection 'skills' using the skill
#data from the local csv file: 'HotSkills.csv'


#load packages
import csv
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId


#load skill data
def loadskill():
    print "loading the hot skills"
    data = []
    with open('HotSkills.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row[1:])

    #get skill names
    skillNames = data[0]
    #get skill coefficients
    skillCoefficients = pd.DataFrame(data[1:])
    #convert to numeric values and convert NA to 0
    skillCoefficients = skillCoefficients.convert_objects(convert_numeric=True).fillna(0)
    #clean coefficients matrix
    skillCoefficients = cleanskill(skillCoefficients)
    #add skill names
    skillCoefficients.columns = skillNames
    return(skillCoefficients)

#clean the skill coefficients
def cleanskill(skillCoefficients):
    #make up the lower left corner
    skillCoefficients = skillCoefficients.add(skillCoefficients.transpose())
    #subtract by the identity matrix due to over-adding along the diagonal
    idMatrix = pd.DataFrame(np.identity(skillCoefficients.shape[0]))
    skillCoefficients = skillCoefficients.subtract(idMatrix)
    return(skillCoefficients)

#insert data into MongoDB
def insertdb(skillCoefficients):
    #get skill names
    skillNames = skillCoefficients.columns
    n = len(skillNames)
    for i in range(n):
        for j in range(n):
            #get the coefficient
            coefficient = skillCoefficients.loc[i,skillNames[j]]
            #insert
            db1.skills.insert({"_id":str(ObjectId()),skillNames[i]:{skillNames[j]:coefficient}})
    print str(n) + " skill coefficients have been uploaded"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    #load skills data
    skillCoefficients = loadskill()
    #insert into mongodb
    insertdb(skillCoefficients)
