#this script predicts the real ratings for beers using the log data collected from the web app
#get sales data from mongodb and get log data from elasticsearch
#the basic idea is shown as below:

#given the practical ratings for beers by customers, train website traffic data,
#such as clicks, reviews, chechouts, subscriptions ..., using machine learning models,
#such as random forests, linear regression and lasso regression to find the "real"
#ratings. So, we can sort by ratings and recommend beers for new customers

#process:
#1, retrieve data from backend databases
#2, merge data from multiple data sources
#3, find the best model by cross validation
#4, finally fit the best model to all beer data and get predicted "real" ratings
#5, update the beer ranks


#Load packages
import csv
import pandas as pd
from pandas import DataFrame
import numpy as np
from numpy import random
import sys
import pymongo
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from sklearn.ensemble import RandomForestRegressor
from sklearn import linear_model
from sklearn import svm


#define the function to get inventory info from mongodb
def getinventory():
    inventoryInfo = []
    for beer in db1.beers.find():
        beerInfoDict = {}
        #get one inventory
        inventory = beer.get("inventory")[0]
        beerInfoDict["status"] = beer.get("status")
        beerInfoDict["vendor"] = beer.get("vendor",{}).get("name","")
        beerInfoDict["tags"] = beer.get("tags",[])
        beerInfoDict["name"] = beer.get("overview",{}).get("name")
        beerInfoDict["beerId"] = inventory.get("beerId","")
        beerInfoDict["vendorId"] = inventory.get("vendorId")
        beerInfoDict["currentPrice"] = inventory.get("price",{}).get("currentPrice")
        #beerInfoDict["rating"] = beer.get("overview",{}).get("rating",0)
        beerInfoDict["size"] = inventory.get("type",{}).get("size")
        beerInfoDict["container"] = inventory.get("type",{}).get("container")
        beerInfoDict["remaining"] = inventory.get("remaining",0)
        inventoryInfo.append(beerInfoDict)
    return(DataFrame(inventoryInfo))

# #define the function to get inventory info
# def getinventory():
#     inventoryInfo = []
#     for beer in db1.beers.find():
#         for inventory in beer.get("inventory",[]):
#             beerInfoDict = {}
#             beerInfoDict["status"] = beer.get("status")
#             beerInfoDict["vendorId"] = beer.get("vendor",{}).get("_id","")
#             beerInfoDict["tags"] = beer.get("tags",[])
#             beerInfoDict["name"] = beer.get("overview",{}).get("name")
#             beerInfoDict["beerId"] = inventory.get("beerId","")
#             beerInfoDict["currentPrice"] = inventory.get("price",{}).get("currentPrice")
#             #beerInfoDict["rating"] = beer.get("overview",{}).get("rating",0)
#             beerInfoDict["size"] = inventory.get("type",{}).get("size")
#             beerInfoDict["container"] = inventory.get("type",{}).get("container")
#             beerInfoDict["remaining"] = inventory.get("remaining",0)
#             inventoryInfo.append(beerInfoDict)
#     return(DataFrame(inventoryInfo))

#define the function to group by data
def groupby(data,row,column,newName="",fun="sum"):
    #aggregate data by a specific function
    if fun == "sum":
        groupedData = data.groupby([row])[column].sum()
    elif fun == "count":
        groupedData = data.groupby([row])[column].count()
    elif fun == "mean":
        groupedData = data.groupby([row])[column].mean()
    #convert the default output series to data frame
    ind = groupedData.index
    groupedData = DataFrame(groupedData)
    groupedData[row] = ind
    #rename the column if necessary
    if newName != "":
        groupedData=groupedData.rename(columns = {column:newName})
    return(groupedData)

#define the function for getting some beer subscriptions info from elasticsearch
def getsubscriptioninfo(indexName,typeName = "logData",maxSize = 10000):
    subscriptionList = []
    #search for all subscription data
    query = {"query":{"match_all":{}}}
    res = es.search(index=indexName, body=query, doc_type=typeName, size=maxSize)
    for row in res['hits']['hits']:
        subscriptionList.append(row['_source'])
    #check data validation
    n = len(subscriptionList)
    if n == 0:
        raise ValueError("miss subscription info")
    else:
        print str(n) + " rows subscription data have been loaded"
    return(DataFrame(subscriptionList))

#define the function for getting some beer clicks info from elasticsearch
def getclickinfo(indexName,typeName = "logData",maxSize = 10000):
    clickList = []
    #search for all types of clicks
    query = {"query":{"match_all":{}}}
    res = es.search(index=indexName, body=query, doc_type=typeName, size=maxSize)
    for row in res['hits']['hits']:
        clickList.append(row['_source'])
    #check data validation
    n = len(clickList)
    if n == 0:
        raise ValueError("miss clicks info")
    else:
        print str(n) + " rows click data have been loaded"
    return(DataFrame(clickList))

#define the function to add click data for all types
def addclickinfo(data,clickInfo,clickTypes):
    for clickType in clickTypes:
        #subset data by type aand join data to the main data source
        clickData = clickInfo.loc[clickInfo.clickType == clickType]
        groupedData = groupby(clickData,row="beerId",column="clicks",newName=clickType+"Clicks",fun="sum")
        data = joindata(data,groupedData,column="beerId")
    return(data)

#define the function for getting some beer checkout info from elasticsearch
def getcartinfo(indexName,typeName = "logData",maxSize = 10000):
    cartList = []
    #search for all cart info
    query = {"query":{"match_all":{}}}
    res = es.search(index=indexName, body=query, doc_type=typeName, size=maxSize)
    for row in res['hits']['hits']:
        cartList.append(row['_source'])
    #check data validation
    n = len(cartList)
    if n == 0:
        raise ValueError("miss carts info")
    else:
        print str(n) + " rows carts data have been loaded"
    return(DataFrame(cartList))

#define the function for getting some beer checkout info from elasticsearch
def getcheckoutinfo(indexName,typeName = "logData",maxSize = 10000):
    checkoutList = []
    #search for all cart info
    query = {"query":{"match_all":{}}}
    res = es.search(index=indexName, body=query, doc_type=typeName, size=maxSize)
    for row in res['hits']['hits']:
        checkoutList.append(row['_source'])
    #check data validation
    n = len(checkoutList)
    if n == 0:
        raise ValueError("miss checkout info")
    else:
        print str(n) + " rows checkout data have been loaded"
    return(DataFrame(checkoutList))

#define the function for getting some beer checkout info from elasticsearch
def getratinginfo(indexName,typeName = "logData",maxSize = 10000):
    ratingList = []
    #search for all cart info
    query = {"query":{"match_all":{}}}
    res = es.search(index=indexName, body=query, doc_type=typeName, size=maxSize)
    for row in res['hits']['hits']:
        ratingList.append(row['_source'])
    #check data validation
    n = len(ratingList)
    if n == 0:
        raise ValueError("miss rating info")
    else:
        print str(n) + " rows ranting data have been loaded"
    return(DataFrame(ratingList))

#define the function for joining two sources to one, the first data source is the main one
def joindata(data1,data2,column,cleanNA = True):
    #left join those two data sources, since the left one is the main data source
    data = pd.merge(data1,data2,on=column,how='left',copy=True)
    #clean all NA's if necessary
    if cleanNA == True:
        data = data.fillna(0)
    return(data)

#define the function to get training data
def gettraining(data,XColumns,YColumns,dropNA = True,shuffle = True):
    #make a copy
    trainingData = data.copy()
    #drop a few columns which we won't use for ML models
    trainingData = trainingData[XColumns+YColumns]
    if dropNA == True:
        #drop Na values if necessary
        trainingData = trainingData.dropna()
    if shuffle == True:
        #shuffle data if necessary
        trainingData = trainingData.reindex(np.random.permutation(trainingData.index))
    return(trainingData)

#define the function to normalize data for lasso & ridge models
def normalize(data):
    #normalize data by mean and standard deviation
    data_norm = (data - data.mean()) / data.std()
    #if the column is a constant, keep the original value
    ind = [i for i, x in enumerate(data.std()) if x == 0]
    if ind != []:
        data_norm.ix[:,ind] = (data - data.mean())
    return(data_norm)

#define the function to calculate the random forest CV error, k = 5 for default
def RFCV(data,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200):
    #make number of rows divisible by 5
    n = data.shape[0]/k*k
    data = data.iloc[range(n)]
    #set up the initial values for these two tuning parameters
    nCandidates = [2,5,20,50,100,200,300,400,500,700,1000]
    numTrees = nCandidates[:nCandidates.index(maxNumTrees)+1]
    depths = range(1,maxDepth+1)
    #first tune depth with initial number of trees
    print "start tuning the max depth"
    depthErrors = []
    for d in depths:
        #begin k-fold CV
        CVtestMSE = 0
        for i in range(k):
            #get training data & test data split
            testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
            trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
            #get test & training & target
            training = trainingData.drop("rating",axis=1)
            target = trainingData["rating"]
            testing = testingData.drop("rating",axis=1)
            #get model
            model = RandomForestRegressor(n_estimators=nTreeInitial,max_depth=d,max_features="sqrt")
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData["rating"])
            CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
        #append test errors
        depthErrors.append(CVtestMSE)
    #get the best maxDepth
    bestDepth = depths[depthErrors.index(min(depthErrors))]
    #then tune number of trees
    print "start tuning the number of trees"
    nErrors = []
    for numTree in numTrees:
        #begin k-fold CV
        CVtestMSE = 0
        for i in range(k):
            #get training data & test data split
            testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
            trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
            #get test & training & target
            training = trainingData.drop("rating",axis=1)
            target = trainingData["rating"]
            testing = testingData.drop("rating",axis=1)
            #get model
            model = RandomForestRegressor(n_estimators=numTree,max_depth=bestDepth,max_features="sqrt")
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData["rating"])
            CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
        #append test errors
        nErrors.append(CVtestMSE)
    #get the best numTrees
    bestNumTree = numTrees[nErrors.index(min(nErrors))]
    print 'Test Mean Squared Error for depths:'
    print depthErrors
    print 'Test Mean Squared Error for number of trees:'
    print nErrors
    print 'The final depth is ' + str(bestDepth)  + ' and the final number of trees is ' + str(bestNumTree) + '.'
    return([[bestDepth,bestNumTree],min(nErrors)])

#define the function to calculate the lasso CV error, k = 5 for default
def lassoCV(data,k = 5,maxAlpha = 10):
    #make number of rows divisible by 5
    n = data.shape[0]/k*k
    data = data.iloc[range(n)]
    #set up the initial values for tuning parameter, alpha
    alphaCandidates = [0.1,0.5,1,2,5,10]
    alphas =  alphaCandidates[:alphaCandidates.index(maxAlpha)+1]
    #tune the penalty parameter alpha
    print "start tuning the penalty parameter, alpha"
    lassoErrors = []
    for a in alphas:
        #begin k-fold CV
        CVtestMSE = 0
        for i in range(k):
            #get training data & test data split
            testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
            trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
            #get test & training & target
            training = trainingData.drop("rating",axis=1)
            target = trainingData["rating"]
            testing = testingData.drop("rating",axis=1)
            #normalize data
            training = normalize(training)
            testing = normalize(testing)
            #get model
            model = linear_model.Lasso(alpha = a,normalize=True)
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData["rating"])
            CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
        #append test errors
        lassoErrors.append(CVtestMSE)
    #get the best alpha
    bestAlpha = alphas[lassoErrors.index(min(lassoErrors))]
    print 'Test Mean Squared Error for alphas:'
    print lassoErrors
    print 'The final alpha is ' + str(bestAlpha) + '.'
    return([bestAlpha,min(lassoErrors)])

#define the function to calculate the ridge CV error, k = 5 for default
def ridgeCV(data,k = 5,maxAlpha = 10):
    #make number of rows divisible by 5
    n = data.shape[0]/k*k
    data = data.iloc[range(n)]
    #set up the initial values for tuning parameter, alpha
    alphaCandidates = [0.1,0.5,1,2,5,10]
    alphas =  alphaCandidates[:alphaCandidates.index(maxAlpha)+1]
    #tune the penalty parameter, alpha
    print "start tuning the penalty parameter, alpha"
    ridgeErrors = []
    for a in alphas:
        #begin k-fold CV
        CVtestMSE = 0
        for i in range(k):
            #get training data & test data split
            testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
            trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
            #get test & training & target
            training = trainingData.drop("rating",axis=1)
            target = trainingData["rating"]
            testing = testingData.drop("rating",axis=1)
            #normalize data
            training = normalize(training)
            testing = normalize(testing)
            #get model
            model = linear_model.Ridge(alpha = a,normalize=True)
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData["rating"])
            CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
        #append test errors
        ridgeErrors.append(CVtestMSE)
    #get the best alpha
    bestAlpha = alphas[ridgeErrors.index(min(ridgeErrors))]
    print 'Test Mean Squared Error for alphas:'
    print ridgeErrors
    print 'The final alpha is ' + str(bestAlpha) + '.'
    return([bestAlpha,min(ridgeErrors)])

#define the function to calculate the linear regression CV error, k = 5 for default
def linearCV(data,k = 5):
    #make number of rows divisible by 5
    n = data.shape[0]/k*k
    data = data.iloc[range(n)]
    #start the CV process
    CVtestMSE = 0
    for i in range(k):
        #get training data & test data split
        testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
        trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
        #get test & training & target
        training = trainingData.drop("rating",axis=1)
        target = trainingData["rating"]
        testing = testingData.drop("rating",axis=1)
        #get model
        model = linear_model.LinearRegression()
        model = model.fit(training,target)
        #evaluate model and compute test error
        pred = np.array(model.predict(testing))
        testY = np.array(testingData["rating"])
        CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
    #get the CV error
    linearError = CVtestMSE
    print 'Test Mean Squared Error for linear regression:'
    print linearError
    return(["",linearError])

#define the function to fit the best model to all data to get ratings
def bestfit(beersData,allData,trainingData,model,parameter):
    #make a copy
    data = allData.copy()
    #fit the best model and make prediction
    #the possible model woule be "random forest", "lasso", "ridge", "linear regression"
    if model == "RF":
        #get parameters
        bestDepth = parameter[0]
        numTree = parameter[1]
        #get test & training & target
        training = trainingData.drop("rating",axis=1)
        target = trainingData["rating"]
        #fit
        model = RandomForestRegressor(n_estimators=numTree,max_depth=bestDepth,max_features="sqrt")
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        beersData["newRating"] = pred
    elif model == "lasso":
        #get parameters
        a = parameter
        #get test & training & target
        training = trainingData.drop("rating",axis=1)
        target = trainingData["rating"]
        #normalize data
        training = normalize(training)
        data = normalize(data)
        #get model
        model = linear_model.Lasso(alpha = a,normalize=True)
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        beersData["newRating"] = pred
    elif model == "ridge":
        #get parameters
        a = parameter
        #get test & training & target
        training = trainingData.drop("rating",axis=1)
        target = trainingData["rating"]
        #normalize data
        training = normalize(training)
        data = normalize(data)
        #get model
        model = linear_model.Ridge(alpha = a,normalize=True)
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        beersData["newRating"] = pred
    elif model == "linear":
        #get test & training & target
        training = trainingData.drop("rating",axis=1)
        target = trainingData["rating"]
        #get model
        model = linear_model.LinearRegression()
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        beersData["newRating"] = pred
    else:
        print "error: model not found"
    return(beersData)

#define the function to generate ranks by sales and price (both desending)
def getrank(beersData):
    #check data validation
    if beersData["newRating"].isnull().any():
        raise ValueError("we have missed some ratings, which is not expected")
    #get rank all beers based on sales and price
    beersData = beersData.sort(["rating","currentPrice"],ascending=[False,False])
    return(beersData)


#below is the main function

#connect to mongodb from local port 3001
print "connecting to mongodb from the local server"
client = MongoClient('localhost', 3001)
db1 = client.meteor
# client1 = MongoClient('localhost', 27017)
# db = client1.appDB

#connect to elasticsearch with the default setting
print "connecting to elasticsearch from the local server"
es = Elasticsearch()

#get all inventory info a as the main data source
print "getting inventory info"
inventoryInfo = getinventory()
beersData = inventoryInfo.copy()

#get subscription info and join
print  "getting subscription"
beerSubsInfo = groupby(getsubscriptioninfo(indexName="subscriptions"),row="beerId",column="userId",newName="beerSubs",fun="count")
beersData = joindata(beersData,beerSubsInfo,column="beerId")

#get click info
print "getting clicks info"
clickTypes = ["review","vendor","similar","regular"]
clickInfo = getclickinfo(indexName="clicks")
beersData = addclickinfo(beersData,clickInfo,clickTypes)

#get add to cart info
print "getting add to cart info"
cartInfo = groupby(getcartinfo(indexName="carts"),row="beerId",column="quantity",newName="cartAdd",fun="sum")
beersData = joindata(beersData,cartInfo,column="beerId")

#get checkout info
print "getting checkout info"
checkoutInfo = groupby(getcheckoutinfo(indexName="checkouts"),row="beerId",column="quantity",newName="checkout",fun="sum")
beersData = joindata(beersData,checkoutInfo,column="beerId")

#get vendor info
print "getting vendor info"
vendorSubsInfo = groupby(getsubscriptioninfo(indexName="subscriptions"),row="vendorId",column="userId",newName="vendorSubs",fun="count")
beersData = joindata(beersData,vendorSubsInfo,column="vendorId")

#get rating info
print "getting rating info"
ratingInfo = groupby(getratinginfo(indexName="ratings"),row="beerId",column="rating",newName="",fun="mean")
beersData = joindata(beersData,ratingInfo,column="beerId",cleanNA = False)

#get shuffled training data, droping a few columns
print "getting training data"
#we don't consider the column "remaining" for now
XColumns = ["currentPrice","beerSubs","reviewClicks","vendorClicks","similarClicks","regularClicks","cartAdd","checkout"]
YColumns = ["rating"]
data = gettraining(beersData,XColumns,YColumns)

#find the best random forest model by cross validation
print "getting the best random forest model"
[RFParameters,RFError] = RFCV(data,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200)

#find the best lasso model by cross validation
print "getting the best lasso model"
[lassoParameter,lassoError] = lassoCV(data,k = 5,maxAlpha = 10)

#find the best rigde model by cross validation
print "getting the best ridge model"
[ridgeParameter,ridgeError] = ridgeCV(data,k = 5,maxAlpha = 10)

#fit the simple linear model by cross validation
print "fitting the simple linear model"
[linearParameter,linearError] = linearCV(data,k = 5)

#print the error for each model and get the best model with smallest CV error
modelTypes = ["RF","lasso","ridge","linear"]
modelParameters = [RFParameters,lassoParameter,ridgeParameter,linearParameter]
modelErrors = [RFError,lassoError,ridgeError,linearError]
bestModel = modelTypes[modelErrors.index(min(modelErrors))]
bestParameter = modelParameters[modelErrors.index(min(modelErrors))]
print "the smallest error for each model:"
print "random forest:"+str(RFError)+", lasso:"+str(lassoError)+", ridge:"+str(ridgeError)+", linear:"+str(linearError)

#fit the best model to all data
print "working on final fit using the best selected model"
XColumns = ["currentPrice","beerSubs","reviewClicks","vendorClicks","similarClicks","regularClicks","cartAdd","checkout"]
YColumns = []
allData = gettraining(beersData,XColumns,YColumns,dropNA = False,shuffle=False)
dataOutput = bestfit(beersData,allData,data,model = bestModel,parameter = bestParameter)
print dataOutput.head(10)

print "generating ranks and insert to mongodb"
#sort beers data based on sales and price
beersData = getrank(dataOutput)
n = beersData.shape[0]
for i in range(n):
    rank = i + 1
    beerId = beersData.iloc[i]["beerId"]
    db1.beers.update({"_id":beerId},{'$set':{"rank":rank}})
print str(i+1) + " ranks have been generated"
