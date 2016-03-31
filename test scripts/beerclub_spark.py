#this script calculates the recommended beers for beer club members
#the basic process is that given some feedback of beers (like or dislike),
#predict beers for final shipping

#Load packages
import csv
import pandas as pd
from pandas import DataFrame
import numpy as np
from numpy import random
import sys
import pymongo
from pymongo import MongoClient
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.classification import LogisticRegressionWithLBFGS

#define the function to get inventory info
def getinventory():
    inventoryInfo = []
    for beer in db.beers.find():
        for inventory in beer.get("inventory",[]):
            beerInfo_dict = {}
            beerInfo_dict["status"] = beer.get("status")
            beerInfo_dict["vendor"] = beer.get("vendor",{}).get("name","")
            beerInfo_dict["tags"] = beer.get("tags",[])
            beerInfo_dict["name"] = beer.get("overview",{}).get("name")
            beerInfo_dict["beerId"] = inventory.get("beerId","")
            beerInfo_dict["current price"] = inventory.get("price",{}).get("currentPrice")
            beerInfo_dict["rating"] = beer.get("overview",{}).get("rating",0)
            beerInfo_dict["size"] = inventory.get("type",{}).get("size")
            beerInfo_dict["container"] = inventory.get("type",{}).get("container")
            beerInfo_dict["remaining"] = inventory.get("remaining",0)
            inventoryInfo.append(beerInfo_dict)
    return(DataFrame(inventoryInfo))

#define the function for getting some beers
def getmenbers(n = 5):
    members_list = []
    for i in range(n):
        j = random.random_integers(0,5)
        member_dict = {}
        member_dict["member id"] = i
        member_dict["type"] = ["exclusive","deluxe","basic"][j % 3]
        member_dict["strength"] = ["strong","medium","weak"][j % 3]
        member_dict["taste"] = ["ipa","sour","porter"][0:(j%3)]+["ipa","sour","porter"][(j%3+1):]
        member_dict["region"] = ["east","mid","west"][j % 3]
        member_dict["initial value"] = [130,100,70][j % 3]
        member_dict["value"] = random.random_integers(0,4) * 5
        member_dict["freedom"] = 4 - 2*(j % 3)
        member_dict["shipping cost"] = 30
        members_list.append(member_dict)
    return(members_list)

#define the function to reshape data
def getsalesdata(data, row = "product_title",value = "total_sales", column = "region"):
    #change data types
    data[["total_sales","price","quantity_count"]] = data[["total_sales","price","quantity_count"]].astype(float)
    data[["quantity_count"]] = data[["quantity_count"]].astype(int)
    #clean data
    data = data.query('shipping_province != "" and product_title != "" and total_sales >= 0')
    data = data.query('product_type != "Gift Card" and product_type != "Subscription" and product_type != ""')
    #subset data only for product title, shipping provingce and the Y variable we want
    data = data[[row,column,value]]
    #reshape data by location
    data = pd.pivot_table(data, index = [row], columns = [column],
                            values = [value], aggfunc = np.sum, fill_value = 0)
    #print data.tail(10)
    #print data.T.to_dict("list")
    return(data.T.to_dict("list"))

#define the function to add sales to inventory data
def addsalesdata(inventoryInfo,salesData):
    #get a copy of inventoryInfo
    data = inventoryInfo.copy()
    #get initial sales lists
    east_list = []
    mid_list = []
    west_list = []
    #add sales data
    for beerName in inventoryInfo["name"]:
        sales = salesData.get(beerName,[0,0,0])
        east_list.append(sales[0])
        mid_list.append(sales[1])
        west_list.append(sales[2])
    data["east"] = east_list
    data["mid"] = mid_list
    data["west"] = west_list
    return(data)

#define the function to get initial recommended beers
def getinitialbeers(members,beersData,minRemaining=20,minRating=95,total=6,profit=10,top=20):
    #make a copy of beers data
    beers = beersData.copy()
    #pre-filter beers by remaining, status and rating
    beers = beers.loc[beers["remaining"] >= minRemaining]
    beers = beers.loc[beers["status"] == "available"]
    #beersData = beersData.loc[beersData["rating"] >= minRating]
    #print beersData.dtypes
    #get recommendation
    recommendation = []
    #get member info
    for member in members:
        print member
        #get member info
        memberId = member.get("member id")
        region = member.get("region")
        freedom = member.get("freedom")
        #filter by strength and taste
        strength = member.get("strength",[])
        taste = member.get("taste",[])
        #if ... then ...
        #get budget
        budget = member.get("initial value") - member.get("shipping cost")
        #get candidates
        totalPrice = 0
        i = 0
        unique = False
        while (budget - totalPrice < profit or unique == False):
            i += 1
            print i
            goodBeers = beers.sort_index(by=[region,'current price','remaining'],ascending=[False,True,False])
            goodBeers = goodBeers.iloc[random.choice(range(top),freedom,False)]
            badBeers = beers.sort_index(by=['current price',region,'remaining'],ascending=[True,False,False])
            badBeers = badBeers.iloc[random.choice(range(top),total-freedom,False)]
            #print goodBeers
            #print goodBeers["current price"].tolist()
            recommendedBeers = goodBeers["beerId"].tolist()+badBeers["beerId"].tolist()
            #see whether satisfy the conditions to proceed
            if len(set(recommendedBeers)) == len(recommendedBeers):
                unique = True
            totalPrice = sum(goodBeers["current price"].tolist()+badBeers["current price"].tolist())
            #print totalPrice
        recommendation.append({"memberId":memberId,"recommendedBeers":recommendedBeers})
    return(recommendation)

#define the function to simulate some feedback
def getfeedback(beersInitial):
    feedback = list(beersInitial)
    for i, member in enumerate(beersInitial):
        recommendedBeers = member.get("recommendedBeers")
        recommendation_list = []
        for beer in recommendedBeers:
            #get random feedback
            like = [1,0,None][random.choice(3,1,p=[0.5,0.4,0.1])]
            recommendation_list.append({beer:like})
        feedback[i]["recommendedBeers"] = recommendation_list
    #print feedback
    return(feedback)

#define the function to clean feedback
def cleanfeedback(feedback):
    feedback_list = []
    for member in feedback:
        for beer in member.get("recommendedBeers"):
            if beer.values()[0] == None:
                continue
            feedback_dict = {}
            feedback_dict["memberId"] = member.get("memberId")
            feedback_dict["beerId"] = beer.keys()[0]
            feedback_dict["like"] = beer.values()[0]
            feedback_list.append(feedback_dict)
    #print DataFrame(feedback_list)
    return(DataFrame(feedback_list))

#define the function to get traing data
def gettraining(feedback,members,inventoryInfo):
    #make copies
    trainingData = feedback.copy()
    inventory = inventoryInfo.copy()
    members = DataFrame(list(members))
    #print members
    #print members
    #add member info to training data
    strength_list = []
    taste_list = []
    region_list = []
    type_list = []
    for memberId in feedback["memberId"]:
        strength_list.append(members.loc[members["member id"] == memberId,"strength"].values[0])
        taste_list.append(members.loc[members["member id"] == memberId,"taste"].values[0])
        region_list.append(members.loc[members["member id"] == memberId,"region"].values[0])
        type_list.append(members.loc[members["member id"] == memberId,"type"].values[0])
    trainingData["strength"] = strength_list
    trainingData["taste"] = taste_list
    trainingData["region"] = region_list
    trainingData["type"] = type_list
    #add beer info to training data
    container_list = []
    price_list = []
    rating_list = []
    remaining_list = []
    size_list = []
    vendor_list = []
    sales_list = []
    #beerId = inventoryInfo["beerId"][0]
    #print inventory.loc[inventory["beerId"] == beerId,"container"].values[0]
    for beerId in feedback["beerId"]:
        container_list.append(inventory.loc[inventory["beerId"] == beerId,"container"].values[0])
        price_list.append(inventory.loc[inventory["beerId"] == beerId,"current price"].values[0])
        rating_list.append(inventory.loc[inventory["beerId"] == beerId,"rating"].values[0])
        remaining_list.append(inventory.loc[inventory["beerId"] == beerId,"remaining"].values[0])
        size_list.append(inventory.loc[inventory["beerId"] == beerId,"size"].values[0])
        vendor_list.append(inventory.loc[inventory["beerId"] == beerId,"vendor"].values[0])
        east = inventory.loc[inventory["beerId"] == beerId,"east"].values[0]
        mid = inventory.loc[inventory["beerId"] == beerId,"mid"].values[0]
        west = inventory.loc[inventory["beerId"] == beerId,"west"].values[0]
        sales_list.append(east+mid+west)
    #print container_list
    trainingData["container"] = container_list
    trainingData["current price"] = price_list
    trainingData["rating"] = rating_list
    trainingData["remaining"] = remaining_list
    trainingData["size"] = size_list
    trainingData["vendor"] = vendor_list
    trainingData["total sales"] = sales_list
    #print inventoryInfo.head(10)
    #print trainingData
    return(trainingData)

#define the function to get the convert_dict for categorical data to int
def getconvertto(x,columns):
    x = DataFrame(x)
    #name = x.columns.values
    convertto_dict = {}
    for column in columns:
        unique = set(x[column])
        convertto = {}
        for value, item in enumerate(unique):
            convertto[item] = value
        convertto_dict[column] = convertto
    return(convertto_dict)

#define the function to drop some columns which are not needed
def reorganize(x,columns,y):
    #make a copy
    data = x.copy()
    #reorder the dataframe and put y to be the first column
    names = list(x.columns.values)
    index = names.index(y)
    names = [names[index]] + names[0:index] + names[(index+1):]
    #drop columns
    for column in columns:
        names.remove(column)
    #print data[names]
    return(data[names])

#define the function to transform categorical data to int
def convertto(x,convertto_dict,columns):
    #make an empty list
    data = []
    m = x.shape[0]
    n = x.shape[1]
    for i in range(m):
        row = []
        for j in range(n):
            name = x.columns.values[j]
            if name in columns:
                row.append(convertto_dict.get(name).get(x.iloc[i,j]))
            else:
                row.append(x.iloc[i,j])
        data.append(row)
    return(data)

#define the function for reordering the training data and get labeled points
def gettrainingpoints(row):
    y = row[0]
    x = row[1:]
    #print x
    return LabeledPoint(y, x)

#define the function for reordering the training data and get labeled points
def gettestingpoints(row):
    y = 0
    x = row
    #print x
    return LabeledPoint(y, x)

#define the function to get the map of categorical features info
def getmap(x,columns):
    x = DataFrame(x)
    names = list(x.columns.values)
    categorical_dict = {}
    for column in columns:
        index = names.index(column)
        unique = set(x[column])
        categorical_dict[index-1] = len(unique)
    #print categorical_dict
    return(categorical_dict)

#define the function to get prediction
def predictRF(traningData,beersData,members,model,convertto_dict,minRemaining=20,top=100,out=20):
    #make copies
    allData = beersData.copy()
    allData["total sales"] = allData["east"] + allData["mid"] + allData["west"]
    allData = allData.sort_index(by=['total sales'],ascending=[False])
    #filter
    allData = allData.loc[allData["remaining"] >= minRemaining]
    allData = allData.loc[allData["status"] == "available"]
    #make predictions
    column_list = ["strength","region"]
    pred_dict = {}
    for member in members:
        #pred_dict = {}
        testData = gettesting(allData,trainingData,member,column_list,top)
        #print testData.head(10)
        test = convertto(testData.ix[:,[0,1,3,4,5,6,7]],convertto_dict,columns=["strength","region","container"])
        test = sc.parallelize(test).map(gettestingpoints)
        #print test.head(10)
        pred = pd.Series(model.predict(test.map(lambda x: x.features)).collect())
        # pred = pd.Series(forest.predict_proba(test)[:,1])
        pred.sort(ascending = False)
        print test.collect()
        print pred
        goodBeers = testData.loc[pred.index.tolist()[0:out],"beerId"].tolist()
        pred_dict[member.get("member id")] = goodBeers
        #pred_dict.append(pred_dict)
    #print pred_list
    #print sc.parallelize(test).map(gettestingpoints).map(lambda lp: lp.label).zip(predictions).collect()
    #print predictions.collect()
    return(pred_dict)

#define the function to get prediction
def predictSVM(traningData,beersData,members,model,convertto_dict,minRemaining=20,top=100,out=20):
    #make copies
    allData = beersData.copy()
    allData["total sales"] = allData["east"] + allData["mid"] + allData["west"]
    allData = allData.sort_index(by=['total sales'],ascending=[False])
    #filter
    allData = allData.loc[allData["remaining"] >= minRemaining]
    allData = allData.loc[allData["status"] == "available"]
    #make predictions
    column_list = ["strength","region"]
    pred_dict = {}
    for member in members:
        #pred_dict = {}
        testData = gettesting(allData,trainingData,member,column_list,top)
        #print testData.head(10)
        test = convertto(testData.ix[:,[0,1,3,4,5,6,7]],convertto_dict,columns=["strength","region","container"])
        test = sc.parallelize(test).map(gettestingpoints)
        #print test.head(10)
        pred = pd.Series(model.predict(test.map(lambda x: x.features)).collect())
        # pred = pd.Series(forest.predict_proba(test)[:,1])
        pred.sort(ascending = False)
        print test.collect()
        print pred
        goodBeers = testData.loc[pred.index.tolist()[0:out],"beerId"].tolist()
        pred_dict[member.get("member id")] = goodBeers
        #pred_dict.append(pred_dict)
    #print pred_list
    #print sc.parallelize(test).map(gettestingpoints).map(lambda lp: lp.label).zip(predictions).collect()
    #print predictions.collect()
    return(pred_dict)

#define the function to get prediction
def predictlogistic(traningData,beersData,members,model,convertto_dict,minRemaining=20,top=100,out=20):
    #make copies
    allData = beersData.copy()
    allData["total sales"] = allData["east"] + allData["mid"] + allData["west"]
    allData = allData.sort_index(by=['total sales'],ascending=[False])
    #filter
    allData = allData.loc[allData["remaining"] >= minRemaining]
    allData = allData.loc[allData["status"] == "available"]
    #make predictions
    column_list = ["strength","region"]
    pred_dict = {}
    for member in members:
        #pred_dict = {}
        testData = gettesting(allData,trainingData,member,column_list,top)
        #print testData.head(10)
        test = convertto(testData.ix[:,[0,1,3,4,5,6,7]],convertto_dict,columns=["strength","region","container"])
        test = sc.parallelize(test).map(gettestingpoints)
        #print test.head(10)
        pred = pd.Series(model.predict(test.map(lambda x: x.features)).collect())
        # pred = pd.Series(forest.predict_proba(test)[:,1])
        pred.sort(ascending = False)
        print test.collect()
        print pred
        goodBeers = testData.loc[pred.index.tolist()[0:out],"beerId"].tolist()
        pred_dict[member.get("member id")] = goodBeers
        #pred_dict.append(pred_dict)
    #print pred_list
    #print sc.parallelize(test).map(gettestingpoints).map(lambda lp: lp.label).zip(predictions).collect()
    #print predictions.collect()
    return(pred_dict)

#define the function to get test data for each menber
def gettesting(allData,training,member,column_list,top):
    #print list(training.columns.values[2:])
    testingData = allData[["beerId"]+list(training.columns.values[3:])][0:top]
    #print testingData.head(10)
    n = testingData.shape[0]
    for column in column_list:
        testingData[column] = [member.get(column)] * top
    #arrange index
    testingData.index = range(top)
    #arrange column
    columns = testingData.columns.tolist()
    columns = columns[len(columns)-2:] + columns[0:len(columns)-2]
    #print columns
    return(testingData[columns])

#define the function recommend beers by our predictions
def getrecommendation(members,predictions,beersData,minRating=95,total=6,profit=10,top=20):
    #make a copy of beers data
    beers = beersData.copy()
    #pre-filter beers by remaining, status and rating
    #beers = beers.loc[beers["remaining"] >= minRemaining]
    #beers = beers.loc[beers["status"] == "available"]
    #beersData = beersData.loc[beersData["rating"] >= minRating]
    #print beersData.dtypes
    #get recommendation
    recommendation = []
    #get member info
    for member in members:
        #print member
        #get member info
        memberId = member.get("member id")
        region = member.get("region")
        freedom = member.get("freedom")
        #filter by strength and taste
        #strength = member.get("strength",[])
        #taste = member.get("taste",[])
        #if ... then ...
        #get budget
        budget = member.get("initial value") - member.get("shipping cost")
        #get candidates
        totalPrice = 0
        i = 0
        unique = False
        while (budget - totalPrice < profit or unique == False):
            i += 1
            print i
            goodBeers = predictions.get(memberId)
            #print goodBeers
            #print random.choice(goodBeers,freedom,False)
            goodBeers = random.choice(goodBeers,freedom,False).tolist()
            badBeers = beers.sort_index(by=['current price',region,'remaining'],ascending=[True,False,False])
            badBeers = badBeers.iloc[random.choice(range(top),total-freedom,False)]
            #print goodBeers
            #print goodBeers["current price"].tolist()
            recommendedBeers = goodBeers + badBeers["beerId"].tolist()
            #see whether satisfy the conditions to proceed
            if len(set(recommendedBeers)) == len(recommendedBeers):
                unique = True
            #get price
            goodPrice = []
            for beerId in goodBeers:
                #a = beers.loc[beers.beerId == beerId,"current price"].tolist()[0]
                goodPrice.append(beers.loc[beers.beerId == beerId,"current price"].tolist()[0])
            #print goodPrice
            totalPrice = sum(goodPrice + badBeers["current price"].tolist())
            #print totalPrice
        recommendation.append({"memberId":memberId,"recommendedBeers":recommendedBeers})
    return(recommendation)

#connect to mongodb
print "connecting mongodb from local server"
client = MongoClient('localhost', 3001)
db = client.meteor

#conf spark
print "configuring spark"
sc = SparkContext("local","spark test")

#get all inventory info
print "getting inventory info"
inventoryInfo = getinventory()
#print inventoryInfo

#print beerIds_dict

#get raw yearly sales data (training data)
print "loading sales data locally"
data = []
with open('all_yearly_beers_sales.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#load into a dataframe
sales = DataFrame(data[1:])
#assign column names
sales.columns = data[0]
#create a column for unique city
sales["shipping_address"] = sales["shipping_city"] + " " + sales["shipping_province"]

#get sales data
print "preparing data"
salesData = getsalesdata(sales)

#add sales  to inventory data
beersData = addsalesdata(inventoryInfo,salesData)

#simulate members info
members = getmenbers(n = 10)

#get initial recommended beers
beersInitial = getinitialbeers(members,beersData)

#get feedback
feedback = cleanfeedback(getfeedback(beersInitial))

#initialize training data
trainingData = gettraining(feedback,members,beersData)
trainingData = reorganize(trainingData,columns=["beerId","memberId","taste","type","size","vendor"],y="like")
#print trainingData
#convert categorical data to int and get categorical info
convertto_dict = getconvertto(trainingData,columns=["strength","region","container"])
categorical_dict = getmap(trainingData,columns=["strength","region","container"])

#get training data
training = convertto(trainingData,convertto_dict,columns=["strength","region","container"])
training = sc.parallelize(training).map(gettrainingpoints)

#print training.collect()
#print training.map(lambda x: x.features).collect()
#print categorical_dict

print "working on predictions"
#get trained and predicted by random forest
# mBins = categorical_dict.get(max(categorical_dict, key = lambda i: categorical_dict[i])) + 1
# modelRF = RandomForest.trainClassifier(training,numClasses=2,categoricalFeaturesInfo=categorical_dict,
#                                     numTrees = 50, featureSubsetStrategy = "auto",
#                                     impurity = 'gini', maxDepth = 2, maxBins = mBins)
# #get testing data and make predictions
# predictions = predictRF(trainingData,beersData,members,modelRF,convertto_dict,top = 200)

#get trained and predicted by random forest
# mBins = categorical_dict.get(max(categorical_dict, key = lambda i: categorical_dict[i])) + 1
# modelRF = RandomForest.trainRegressor(training,categoricalFeaturesInfo=categorical_dict,
#                                     numTrees = 500, featureSubsetStrategy = "auto",
#                                     impurity = 'variance', maxDepth = 2, maxBins = mBins)
# #get testing data and make predictions
# predictions = predictRF(trainingData,beersData,members,modelRF,convertto_dict,top = 200)

#debug
# for data in training.collect():
#     print data
#print(modelRF.toDebugString())

#get trained and predicted by SVM
# modelSVM = SVMWithSGD.train(training, iterations=1000)
# #get testing data and make predictions
# predictions = predictSVM(trainingData,beersData,members,modelSVM,convertto_dict,top = 200)

#get trained and predicted by logistic regression
# modellogistic = LogisticRegressionWithLBFGS.train(training)
# #get testing data and make predictions
# predictions = predictlogistic(trainingData,beersData,members,modellogistic,convertto_dict,top = 200)

print "done"

# get recommended beers
# recommendations = getrecommendation(members,predictions,beersData,profit=5)
#
# print recommendations
