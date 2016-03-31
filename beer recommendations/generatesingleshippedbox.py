#this script generates a shipped beer box for one specific beer club member
#we need to input the shipped box id
#recommended beers are based on region, taste, strength, type of the user
#and price, remaining, popularity of beers and feedback from propose boxes
#The input collections: "beers","rawSalesData","inventory","locationReference","beerBox","proposeBox" and "shippedBox"
#The outputs will be add into "beerBox" and "shippedBox"
#machine learning algorithmes such as random forests, lasso, rigde and linear regression


#Load packages
import logging
import graypy
import csv
from datetime import datetime
import pandas as pd
from pandas import DataFrame
import numpy as np
from numpy import random
import sys
import pymongo
from pymongo import MongoClient
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn import linear_model
from sklearn import svm
from bson.objectid import ObjectId


#define the function to get inventory info from mongodb
def getinventory():
    inventoryInfo = []
    for beer in db1.beers.find():
        beerInfoDict = {}
        #get inventory
        inventory = beer.get("inventory")[0]
        beerInfoDict["status"] = beer.get("status")
        beerInfoDict["vendor"] = beer.get("vendor",{}).get("name","")
        beerInfoDict["tags"] = beer.get("tags",[])
        beerInfoDict["name"] = beer.get("overview",{}).get("name")
        beerInfoDict["beerId"] = inventory.get("beerId","")
        beerInfoDict["currentPrice"] = inventory.get("price",{}).get("currentPrice")
        beerInfoDict["rating"] = beer.get("overview",{}).get("rating",0)
        beerInfoDict["size"] = inventory.get("type",{}).get("size")
        beerInfoDict["container"] = inventory.get("type",{}).get("container")
        beerInfoDict["remaining"] = inventory.get("remaining",0)
        inventoryInfo.append(beerInfoDict)
    return(DataFrame(inventoryInfo))

#define the function to get the location reference list
def getlocationreference():
    beerRegion = {}
    for location in db1.locationReference.find():
        beerRegion[location.get("locationName")] = location.get("locationList")
    return(beerRegion)

#define the function for getting shipped box info
def getshippedbox(shippedBoxId):
    shippedBoxUserIdList = []
    for shippedBoxUser in db1.shippedBox.find({"_id":shippedBoxId}):
        shippedBoxUserId = shippedBoxUser.get("userId")
        shippedBoxId = shippedBoxUser.get("_id")
        proposeBoxId = shippedBoxUser.get("proposeBoxId")
        shippedBoxUserIdList.append({"userId":shippedBoxUserId,"shippedBoxId":shippedBoxId,"proposeBoxId":proposeBoxId})
    return(shippedBoxUserIdList)

#define the function for getting shipped box user info
def getshippedboxuser(shippedBoxUserIdList):
    if shippedBoxUserIdList == []:
        raise ValueError("no shipped box users found")
        boxUsersList = []
    else:
        for boxUser in shippedBoxUserIdList:
            boxUsersList = []
            #get shipped box user id
            boxUserId = boxUser.get("userId")
            shippedBoxId = boxUser.get("shippedBoxId")
            proposeBoxId = boxUser.get("proposeBoxId")
            #get shipped box user info from beer club collection
            boxUser = db1.beerClub.find_one({"subscriber.userId":boxUserId})
            boxUserDict = {}
            boxUserDict["userId"] = boxUserId
            boxUserDict["shippedBoxId"] = shippedBoxId
            boxUserDict["type"] = boxUser.get("box").get("type")
            #get user strength preferences
            boxUserDict["strengthLight"] = boxUser.get("preferences",{}).get("strength",{}).get("light")
            boxUserDict["strengthMedium"] = boxUser.get("preferences",{}).get("strength",{}).get("medium")
            boxUserDict["strengthStrong"] = boxUser.get("preferences",{}).get("strength",{}).get("strong")
            #get user taste preferences, such as stout, IPA, porter, sour
            boxUserDict["tasteStout"] = boxUser.get("preferences",{}).get("taste",{}).get("stout")
            boxUserDict["tasteIPA"] = boxUser.get("preferences",{}).get("taste",{}).get("ipa")
            boxUserDict["tastePorter"] = boxUser.get("preferences",{}).get("taste",{}).get("porter")
            boxUserDict["tasteSour"] = boxUser.get("preferences",{}).get("taste",{}).get("sour")
            #get user region preferences, such as west coast, east coast, mid west and europe
            boxUserDict["regionWestCoast"] = boxUser.get("preferences",{}).get("region",{}).get("westCoast")
            boxUserDict["regionEastCoast"] = boxUser.get("preferences",{}).get("region",{}).get("eastCoast")
            boxUserDict["regionMidWest"] = boxUser.get("preferences",{}).get("region",{}).get("midWest")
            boxUserDict["regionEurope"] = boxUser.get("preferences",{}).get("region",{}).get("europe")
            #set up some other attributes
            boxUserDict["initialValue"] = 130 - 30*(["exclusive","deluxe","basic"].index(boxUserDict.get("type")))
            boxUserDict["value"] = random.random_integers(0,4) * 5
            boxUserDict["freedom"] = 4 - 2*(["exclusive","deluxe","basic"].index(boxUserDict.get("type")))
            boxUserDict["shippingCost"] = 30
            boxUserDict["packagingCost"] = 10
            boxUserDict["shippedBoxNumber"] = db1.shippedBox.find({"userId":boxUserId}).count()
            #get liked beers and disliked beers from propose box collection
            proposeBoxInfo = db1.proposeBox.find_one({"_id":proposeBoxId})
            likedBeers = proposeBoxInfo.get("likedBeers")
            dislikedBeers = proposeBoxInfo.get("dislikedBeers")
            boxUserDict["likedBeers"] = likedBeers
            boxUserDict["dislikedBeers"] = dislikedBeers
            print "user '" + str(boxUserId) + "' has " + str(len(likedBeers)) + " liked beers and " + str(len(dislikedBeers)) + " disliked beers"
            boxUsersList.append(boxUserDict)
    return(boxUsersList)

#define the function to get sales data from mongodb
def getsalesdata():
    sales = []
    for transaction in db1.rawSalesData.find():
        sales.append(transaction)
    sales = DataFrame(sales)
    #create a column for unique city
    sales["shippingAddress"] = sales["shippingCity"] + " " + sales["shippingProvince"]
    return(sales)

#define the function to clean sales data
def cleansalesdata(salesData,row = "productTitle",value = "totalSales",column = "region"):
    #change data types
    salesData[["totalSales","price","quantityCount"]] = salesData[["totalSales","price","quantityCount"]].astype(float)
    salesData[["quantityCount"]] = salesData[["quantityCount"]].astype(int)
    #exclude missing values and non-beers data
    salesData = salesData.query('shippingProvince != "" and productTitle != "" and totalSales >= 0')
    salesData = salesData.query('productType != "Gift Card" and productType != "Subscription" and productType != ""')
    #subset data only for product title, region and the Y variable we want, which is sales
    salesData = salesData[[row,column,value]]
    #group data by product and total sales
    salesData = pd.pivot_table(salesData,index = [row],values = [value],aggfunc = np.sum,fill_value = 0)
    #output as a dictionary and get rid of the key name
    salesDataDict = salesData.to_dict().get(value)
    return(salesDataDict)

#define the function to add sales to inventory data
def addsalesdata(inventoryInfo,salesData):
    #get a copy of inventoryInfo
    inventoryData = inventoryInfo.copy()
    #get initial sales lists
    totalSalesList = []
    #merge sales data to the inventory list
    for beerName in inventoryInfo["name"]:
        sales = salesData.get(beerName,0)
        totalSalesList.append(sales)
    inventoryData["totalSales"] = totalSalesList
    return(inventoryData)

#define the function to get rated boxes with k beers
def getratedbox():
    ratedBoxList = []
    for ratedBoxUser in db1.proposeBox.find({"rated":True}):
        ratedBoxUserDict = {}
        ratedBoxUserDict["proposeBoxId"] = ratedBoxUser.get("_id")
        ratedBoxUserDict["userId"] = ratedBoxUser.get("userId")
        proposeBeers = ratedBoxUser.get("beers")
        likedBeers = ratedBoxUser.get("likedBeers")
        dislikedBeers = ratedBoxUser.get("dislikedBeers")
        restBeers = [beer for beer in proposeBeers if beer not in likedBeers+dislikedBeers]
        ratedBoxUserDict["likedBeers"] = likedBeers
        ratedBoxUserDict["dislikedBeers"] = dislikedBeers
        ratedBoxUserDict["restBeers"] = restBeers
        ratedBoxList.append(ratedBoxUserDict)
    return(ratedBoxList)

#define the fucntion convert ratedbox data a data frame, where like = 1, dislike = 0
def cleanratedbox(ratedBoxInfoRaw):
    ratedBoxList = []
    #get data for each user
    for user in ratedBoxInfoRaw:
        userId = user.get("userId")
        likedBeers = user.get("likedBeers")
        dislikedBeers = user.get("dislikedBeers")
        restBeers = user.get("restBeers")
        #get liked beers
        if likedBeers != []:
            for likedBeer in likedBeers:
                ratedBoxDict = {}
                ratedBoxDict["userId"] = userId
                ratedBoxDict["beerId"] = likedBeer
                ratedBoxDict["like"] = 1
                ratedBoxList.append(ratedBoxDict)
        #get disliked beers
        if dislikedBeers != []:
            for dislikedBeer in dislikedBeers:
                ratedBoxDict = {}
                ratedBoxDict["userId"] = userId
                ratedBoxDict["beerId"] = dislikedBeer
                ratedBoxDict["like"] = -1
                ratedBoxList.append(ratedBoxDict)
        #get the rest beers
        if restBeers != []:
            for restBeer in restBeers:
                ratedBoxDict = {}
                ratedBoxDict["userId"] = userId
                ratedBoxDict["beerId"] = restBeer
                ratedBoxDict["like"] = 0
                ratedBoxList.append(ratedBoxDict)
    return(DataFrame(ratedBoxList))

#define the function to check all data inputs
def checkinputs(inventoryInfo,beersData,shippedBox,userInfo,ratedBoxInfo,shippedBoxId):
    #check shipped box id
    if db1.shippedBox.find_one({"_id":shippedBoxId}) == None:
        raise ValueError("the shipped box doesn't exit")
    #check inventoryId
    if len(inventoryInfo) < 1:
        raise ValueError("failed to load any inventories")
    #check beers data
    if beersData.shape[0] < 1:
        raise ValueError("failed to merge inventory data with sales data")
    #check box users data
    if len(userInfo) < 1:
        raise ValueError("failed to load any propose box which needs to be machine learned")
    #check rated box info
    if ratedBoxInfo.shape[0] < 5:
        raise ValueError("not enough training data, need at least 5 rated beers")
    #print beers data info
    print str(beersData.shape[0]) + " beers loaded"
    print str(ratedBoxInfo.shape[0]) + " rated beers loaded"
    #print users info
    print str(len(userInfo)) + " beer club users loaded and the detail is shown as below:"
    if len(shippedBox) == len(userInfo):
        print userInfo
    else:
        print "warning: one user may have multiple shipped boxes or some blub user info could not be found"

#define the function to get training data for ML model
def gettraining(boxData,beersData,userInfo,columns,categories):
    #make a copy
    data = boxData.copy()
    #join inventory info
    data = pd.merge(data,beersData,on="beerId",how='inner',copy=True)
    data = pd.merge(data,DataFrame(userInfo),on="userId",how='inner',copy=True)
    #drop useless columns
    data = data[columns]
    #convert categorical data to dummy variables
    if categories != []:
        data = getdummy(data,categories,stage = "training",shuffle=True)
    return(data)

#define the function to convert categorical data to dummy variables
def getdummy(rawData,categories,stage,shuffle=False):
    #make a copy
    data = rawData.copy()
    if stage == "training":
        for category in categories:
            columns = list(data.columns.values)
            columnValues = set(data[category])
            dummy = pd.get_dummies(data[category],prefix=category)
            if dummy.shape[1] > 1:
                columns.remove(category)
                data = data[columns].join(dummy.ix[:,1:])
            elif dummy.shape[1] == 1:
                columns.remove(category)
                data = data[columns].join(dummy)
        #shuffle data
        if shuffle == True:
            data = shuffledata(data)
    if stage == "testing":
        columns = list(data.columns.values)
        for category in categories:
            columnValues = set(data[category])
            dummy = pd.get_dummies(data[category],prefix=category)
            dummyColumns = list(dummy.columns.values)
            for dummyColumn in dummyColumns:
                if dummyColumn in columns:
                    data[dummyColumn] = dummy[dummyColumn]
            columns.remove(category)
        data = data[columns]
    return(data)

#define the function to get training data
def shuffledata(trainingDatadata):
    #make a copy
    data = trainingDatadata.copy()
    #shuffle data if necessary
    data = data.reindex(np.random.permutation(data.index))
    return(data)

#define the function to normalize data SVM
def normalize(data):
    #normalize data by mean and standard deviation
    data_norm = (data - data.mean()) / data.std()
    #if the column is a constant, keep the original value
    ind = [i for i, x in enumerate(data.std()) if x == 0]
    if ind != []:
        data_norm.ix[:,ind] = (data - data.mean())
    return(data_norm)

#define the function to calculate the random forest CV error, k = 5 for default
def RFCV(data,Y,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200):
    #make number of rows divisible by 5
    n = data.shape[0]/k*k
    data = data.iloc[range(n)]
    #set up the initial values for these two tuning parameters
    nCandidates = [2,5,20,50,100,200,300,400,500,700,1000]
    numTrees = nCandidates[:nCandidates.index(maxNumTrees)+1]
    depths = range(1,maxDepth+1)
    #first tune depth with initial number of trees
    print "start tuning the max depth for random forest"
    depthErrors = []
    for d in depths:
        #begin k-fold CV
        CVtestMSE = 0
        for i in range(k):
            #get training data & test data split
            testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
            trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
            #get test & training & target
            training = trainingData.drop(Y,axis=1)
            target = trainingData[Y]
            testing = testingData.drop(Y,axis=1)
            #get model
            model = RandomForestRegressor(n_estimators=nTreeInitial,max_depth=d,max_features="sqrt")
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[Y])
            CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
        #append test errors
        depthErrors.append(CVtestMSE)
    #get the best maxDepth
    bestDepth = depths[depthErrors.index(min(depthErrors))]
    #then tune number of trees
    print "start tuning the number of trees for random forest"
    nErrors = []
    for numTree in numTrees:
        #begin k-fold CV
        CVtestMSE = 0
        for i in range(k):
            #get training data & test data split
            testingData = data.iloc[range(i*n/k,(i+1)*n/k)]
            trainingData = data.iloc[range(0,i*n/k)+range((i+1)*n/k,n)]
            #get test & training & target
            training = trainingData.drop(Y,axis=1)
            target = trainingData[Y]
            testing = testingData.drop(Y,axis=1)
            #get model
            model = RandomForestClassifier(n_estimators=numTree,max_depth=bestDepth,max_features="sqrt")
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[Y])
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
def lassoCV(data,Y,k = 5,maxAlpha = 10):
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
            training = trainingData.drop(Y,axis=1)
            target = trainingData[Y]
            testing = testingData.drop(Y,axis=1)
            #normalize data
            training = normalize(training)
            testing = normalize(testing)
            #get model
            model = linear_model.Lasso(alpha = a,normalize=True)
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[Y])
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
def ridgeCV(data,Y,k = 5,maxAlpha = 10):
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
            training = trainingData.drop(Y,axis=1)
            target = trainingData[Y]
            testing = testingData.drop(Y,axis=1)
            #normalize data
            training = normalize(training)
            testing = normalize(testing)
            #get model
            model = linear_model.Ridge(alpha = a,normalize=True)
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[Y])
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
def linearCV(data,Y,k = 5):
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
        training = trainingData.drop(Y,axis=1)
        target = trainingData[Y]
        testing = testingData.drop(Y,axis=1)
        #get model
        model = linear_model.LinearRegression()
        model = model.fit(training,target)
        #evaluate model and compute test error
        pred = np.array(model.predict(testing))
        testY = np.array(testingData[Y])
        CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
    #get the CV error
    linearError = CVtestMSE
    print 'Test Mean Squared Error for linear regression:'
    print linearError
    return(["",linearError])

#define the function to predict liked beers
def bestfit(beersData,trainingData,userInfo,columns,categories,model,parameters,top,value="totalSales",out=20):
    #make a copy
    data = beersData.copy()
    #get column names of trainingData
    trainingColumns = list(trainingData.columns.values)
    trainingColumns.remove("like")
    #only fit for top beers
    data = data.sort_index(by=[value],ascending=[False])
    data = data.iloc[range(top)]
    #get test & training & target
    training = trainingData.drop("like",axis=1)
    target = trainingData["like"]
    #fit the best model and make prediction
    #the possible model woule be "random forest", "lasso", "ridge", "linear regression"
    if model == "RF":
        #get model parameters
        d = parameters[0]
        numTree = parameters[1]
        #fit model
        model = RandomForestClassifier(n_estimators=numTree,max_depth=d,max_features="sqrt")
        model = model.fit(training,target)
        #get test data per user & predict liked beers
        predDict = {}
        userData = DataFrame(userInfo)
        for i in range(len(userInfo)):
            user = DataFrame(userData.iloc[i]).transpose()
            testData = gettestdata(data,user,columns,categories,trainingColumns,top)
            #drop beer id
            test = testData.drop("beerId",axis=1)
            #predict the likelihood
            pred = pd.Series(model.predict(test))
            #sort by likelihood
            pred.sort(ascending = False)
            #get beers with higher likelihood
            goodBeers = testData.loc[pred.index.tolist()[0:out],"beerId"].tolist()
            predDict[user["userId"].values[0]] = goodBeers
    elif model == "lasso":
        #get parameters
        a = parameters
        #get model
        model = linear_model.Lasso(alpha = a,normalize=True)
        model = model.fit(training,target)
        #get test data per user & predict liked beers
        predDict = {}
        userData = DataFrame(userInfo)
        for i in range(len(userInfo)):
            user = DataFrame(userData.iloc[i]).transpose()
            testData = gettestdata(data,user,columns,categories,trainingColumns,top)
            #drop beer id
            test = testData.drop("beerId",axis=1)
            #predict the likelihood
            pred = pd.Series(model.predict(test))
            #sort by likelihood
            pred.sort(ascending = False)
            #get beers with higher likelihood
            goodBeers = testData.loc[pred.index.tolist()[0:out],"beerId"].tolist()
            predDict[user["userId"].values[0]] = goodBeers
    elif model == "ridge":
        #get parameters
        a = parameters
        #get model
        model = linear_model.Lasso(alpha = a,normalize=True)
        model = model.fit(training,target)
        #get test data per user & predict liked beers
        predDict = {}
        userData = DataFrame(userInfo)
        for i in range(len(userInfo)):
            user = DataFrame(userData.iloc[i]).transpose()
            testData = gettestdata(data,user,columns,categories,trainingColumns,top)
            #drop beer id
            test = testData.drop("beerId",axis=1)
            #predict the likelihood
            pred = pd.Series(model.predict(test))
            #sort by likelihood
            pred.sort(ascending = False)
            #get beers with higher likelihood
            goodBeers = testData.loc[pred.index.tolist()[0:out],"beerId"].tolist()
            predDict[user["userId"].values[0]] = goodBeers
    elif model == "linear":
        #get model
        model = linear_model.Lasso(alpha = a,normalize=True)
        model = model.fit(training,target)
        #get test data per user & predict liked beers
        predDict = {}
        userData = DataFrame(userInfo)
        for i in range(len(userInfo)):
            user = DataFrame(userData.iloc[i]).transpose()
            testData = gettestdata(data,user,columns,categories,trainingColumns,top)
            #drop beer id
            test = testData.drop("beerId",axis=1)
            #predict the likelihood
            pred = pd.Series(model.predict(test))
            #sort by likelihood
            pred.sort(ascending = False)
            #get beers with higher likelihood
            goodBeers = testData.loc[pred.index.tolist()[0:out],"beerId"].tolist()
            predDict[user["userId"].values[0]] = goodBeers
    else:
        raise ValueError("error: model not found")
    return(predDict)

#define the function to get test data for each user
def gettestdata(testData,user,columns,categories,trainingColumns,top):
    #make a copy
    data = testData.copy()
    #merge userInfo
    data["userId"] = list(user["userId"]) * top
    test = pd.merge(data,user,on="userId",how='inner',copy=True)
    #drop columns
    test = test[columns+["beerId"]]
    #get test data
    if categories != []:
        #get all trainingColumns
        for trainingColumn in trainingColumns:
            if trainingColumn not in columns:
                test[trainingColumn] = 0
        #get dummy variables
        test = getdummy(test,categories,stage = "testing")
    return(test)

#define the function recommend beers by our predictions
def getrecommendation(userInfo,predictions,beersData,minRemaining=20,total=6,profitRange=[-20,100],top=100):
    #make a copy of beers data
    beers = beersData.copy()
    #pre-filter beers by remaining, status and rating (not finished)
    #beers = beers.loc[beers["remaining"] >= minRemaining]
    #beers = beers.loc[beers["status"] == "available"]
    #get recommendation per user
    recommendation = []
    for i,user in enumerate(userInfo):
        #get user info
        userId = user.get("userId")
        region = user.get("region")
        freedom = user.get("freedom")
        strength = user.get("strength")
        taste = user.get("taste")
        userType = user.get("type")
        #get proce range for different users
        profitRange = getprofitrange(user,profitRange)
        minProfit = profitRange[0]
        maxProfit = profitRange[1]
        #get liked & disliked beers
        likedBeers = user.get("likedBeers")
        dislikedBeers = user.get("dislikedBeers")
        #if the user likes all beersData, skip the process
        if len(likedBeers) == total:
            recommendedBeers = likedBeers
        else:
            #add some beers this user may like
            #update freedom & total by liked beers
            freedom = int(round(freedom*(total - len(likedBeers))/total))
            #update beer candidates by filtering out those dislikedBeers
            for dislikedBeerId in dislikedBeers:
                beers = beers.loc[beers.beerId != dislikedBeerId]
            #get budget
            budget = user.get("initialValue") - user.get("shippingCost") - user.get("packagingCost")
            #set inital values
            totalPrice = 0
            counter = 0
            unique = False
            #recommend beers only if they are in the profit range, and also unique
            while (budget - totalPrice < minProfit or budget - totalPrice > maxProfit or unique == False):
                counter += 1
                #get good beers and price
                goodBeers = predictions.get(userId)
                goodBeers = random.choice(goodBeers,freedom,False).tolist() + likedBeers
                goodPrice = []
                for beerId in goodBeers:
                    goodPrice.append(beers.loc[beers.beerId == beerId,"currentPrice"].tolist()[0])
                #if we fulfill the request by adding some popular beers as bad beers
                if counter < 100:
                    badBeers = beers.sort_index(by=['totalSales','currentPrice','remaining'],ascending=[False,True,False])
                    badBeers = badBeers.iloc[random.choice(range(top),total-freedom-len(likedBeers),False)]
                #if we repeat the process too many times, we need to fulfill the price request compulsively
                else:
                    badBeers = fulfillbox(beers,budget,minProfit,maxProfit,goodPrice,total)
                    badBeers = badBeers.iloc[random.choice(range(badBeers.shape[0]),total-freedom-len(likedBeers),False)]
                #get recommended beers and the total price
                recommendedBeers = goodBeers + badBeers["beerId"].tolist()
                totalPrice = sum(goodPrice + badBeers["currentPrice"].tolist())
                #check for uniqueness
                if len(set(recommendedBeers)) == len(recommendedBeers):
                    unique = True
                print "generating the shipped beer box with total price " + str(totalPrice) + " for " + str(userType) + " box user '" + str(userId) + "' for attempt " + str(counter)
                #if repeat too many times, we have to stop the process
                if counter > 200:
                    raise ValueError("too many attempts, could not fulfill the price requirement")
        #get inventory id
        inventoryIds = getinventoryid(user,recommendedBeers)
        recommendedBeersDict = {}
        recommendedBeersDict["shippedBoxId"] = user.get("shippedBoxId")
        recommendedBeersDict["userId"] = userId
        recommendedBeersDict["type"] = userType
        recommendedBeersDict["recommendedBeers"] = recommendedBeers
        recommendedBeersDict["inventoryIds"] = inventoryIds
        recommendation.append(recommendedBeersDict)
    return(recommendation)

#define the function to get the profit range
def getprofitrange(user,profitRange):
    if user.get("shippedBoxNumber") == 1:
        profitRange = [-20,0]
    if user.get("shippedBoxNumber") == 2:
        profitRange = [-15,5]
    return(profitRange)

#define the function to compulsively fulfill the beer box
def fulfillbox(beers,budget,minProfit,maxProfit,goodPrice,total):
    #make a copy
    badBeers = beers.copy()
    #get the number of good beers already generated
    n = len(goodPrice)
    #get the price range
    maxPrice = (budget - minProfit - sum(goodPrice))/(total-n)
    minPrice = (budget - maxProfit - sum(goodPrice))/(total-n)
    #get beers which can fulfill the request
    badBeers = badBeers.loc[badBeers.currentPrice <= maxPrice]
    badBeers = badBeers.loc[badBeers.currentPrice >= minPrice]
    if badBeers.shape[0] < 1:
        raise ValueError("cannot fulfill the price request by current inventories")
    return(badBeers)

#define the function to get inventory id
def getinventoryid(user,recommendedBeers):
    inventoryIdList = []
    for beerId in recommendedBeers:
        #for some beers having multiple styles, choose the cheapest one as the default
        inventoryId = list(db1.inventory.aggregate([ {"$match":{"beerId":beerId}}, {"$sort":{"price.currentPrice":1}},{"$limit":1},{"$project":{"_id":1}}]))
        inventoryIdList.append(str(inventoryId[0].get("_id")))
    return(inventoryIdList)

#insert into mongodb
def insertdb(recommendedBeers):
    print "starting inserting recommended beers to the beerBox collection:"
    for recommendedBeer in recommendedBeers:
        #get all those beer box info
        print recommendedBeer
        userId = recommendedBeer.get("userId")
        shippedBoxId = recommendedBeer.get("shippedBoxId")
        beers = recommendedBeer.get("recommendedBeers")
        inventoryIds = recommendedBeer.get("inventoryIds")
        userType = recommendedBeer.get("type")
        #insert new beer box to "beerBox" collection if it doesn't exit
        #otherwise get the id and append the user to the existing beer box
        beerBox = db1.beerBox.find_one({"beers":{"$all":beers}})
        if beerBox != None:
            beerBoxId = beerBox.get("_id")
            db1.beerBox.update({"_id":beerBoxId},{"$addToSet":{"toUserPropose":userId}})
        else:
            beerBoxId = db1.beerBox.insert({"_id":str(ObjectId()),"beers":beers,"type":userType,"toUserPropose":[userId]})
        #change the object to string
        beerBoxId = str(beerBoxId)
        #insert recommended beers to propose beer box
        db1.shippedBox.update({"_id":shippedBoxId},{'$set':{"beerBoxId":beerBoxId,"generateType":"machine","toMachineLearn":False,"machineLearnedAt":datetime.utcnow(),"beers":beers,"inventoryIds":inventoryIds}})
    print "done"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    # client1 = MongoClient('localhost', 27017)
    # db1 = client1.appDB

    #define a shipped box id (for debug only)
    #shippedBoxId = "n97ryKBhSBCrHAaZr"

    #get all inventory info
    print "loading sales info and beer box user info"
    inventoryInfo = getinventory()
    #convert raw data to a dictionary locally
    locationReference = getlocationreference()
    #get sales data
    salesData = cleansalesdata(getsalesdata())
    #add sales data to inventory data
    beersData = addsalesdata(inventoryInfo,salesData)
    #get beer box users info
    print "getting beer box user info"
    shippedBox = getshippedbox(shippedBoxId)
    userInfo = getshippedboxuser(shippedBox)
    #get rated box info
    print "getting rated beer box info"
    ratedBoxInfoRaw = getratedbox()
    ratedBoxInfo = cleanratedbox(ratedBoxInfoRaw)
    #check all inputs
    checkinputs(inventoryInfo,beersData,shippedBox,userInfo,ratedBoxInfo,shippedBoxId)
    #get training data
    XColumns = ["container","currentPrice","totalSales","initialValue","strengthLight","strengthMedium","strengthStrong","tasteStout","tasteIPA","tastePorter","regionWestCoast","regionEastCoast","regionMidWest","regionEurope"]
    [YColumn,Y] = [["like"],"like"]
    categories = ["container"]
    trainingData = gettraining(ratedBoxInfo,beersData,userInfo,columns=XColumns+YColumn,categories=categories)
    #find the best model by cross validation
    #algorithme 1: random forest
    print "getting the best random forest model"
    [RFParameters,RFError] = RFCV(trainingData,Y,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200)
    #find the best lasso model by cross validation
    #algorithme 2: lasso regression
    print "getting the best lasso model"
    [lassoParameter,lassoError] = lassoCV(trainingData,Y,k = 5,maxAlpha = 10)
    #find the best rigde model by cross validation
    #algorithme 3: ridge regression
    print "getting the best ridge model"
    [ridgeParameter,ridgeError] = ridgeCV(trainingData,Y,k = 5,maxAlpha = 10)
    #fit the simple linear model by cross validation
    #algorithme 4: linear regression
    print "fitting the simple linear model"
    [linearParameter,linearError] = linearCV(trainingData,Y,k = 5)
    #print the error for each model and get the best model with smallest CV error
    modelTypes = ["RF","lasso","ridge","linear"]
    modelParameters = [RFParameters,lassoParameter,ridgeParameter,linearParameter]
    modelErrors = [RFError,lassoError,ridgeError,linearError]
    bestModel = modelTypes[modelErrors.index(min(modelErrors))]
    bestParameters = modelParameters[modelErrors.index(min(modelErrors))]
    print "the smallest error for each model:"
    print "random forest:"+str(RFError)+", lasso:"+str(lassoError)+", ridge:"+str(ridgeError)+", linear:"+str(linearError)
    #fit the best model to all data
    print "working on final fit using the best model"
    predictions = bestfit(beersData,trainingData,userInfo,XColumns,categories,model=bestModel,parameters=bestParameters,top=200,value="totalSales",out=20)
    #get beer recommendations
    print "working on generating recommended beers"
    recommendedBeers = getrecommendation(userInfo,predictions,beersData,minRemaining=20,total=6,profitRange=[-20,100],top=100)
    #insert shipped beer boxes
    insertdb(recommendedBeers)
