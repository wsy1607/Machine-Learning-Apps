#This script helps to update the match score
#Step 1: verify the position id
#Step 2: get all profiles with feedback associated with the position
#Step 3: update the scores by the best machine learning model
#Step 4: update the scores back to mongodb

#important: for this version, the list of companies and the list of top schools are imported locally
#later on, we will query from mongodb instead


#Load packages
import re
import time
import csv
import pymongo
import pandas
from pandas import DataFrame
from pymongo import MongoClient
import numpy as np
from numpy import random
from sklearn.ensemble import RandomForestRegressor
from sklearn import linear_model
from sklearn import svm
from bson.objectid import ObjectId


#define the function to check the inputs
def checkinputs(positionId,userId):
    position = db1.positions.find_one({"_id":positionId})
    if position == None:
        raise ValueError("the position doesn't exist")
    else:
        positionTitle = position.get("jobOverview").get("title")
    user = db1.profiles.find_one({"_id":userId})
    if user == None:
        raise ValueError("the user doesn't exist")
    else:
        userName = user.get("name")
        userNumConnections = db1.profiles.find({"connectionIds":{"$elemMatch":{"$eq":userId}}}).count()
        print "calculating match score for the position: " + positionTitle + " using " + userName + "'s " + str(userNumConnections) + " connections"

#define the function for sorting recommended connections and return top k per user
def sortbyuser(x, k = 20):
    sortedByKeyList = sorted(x["connections"],key = lambda t: t["score"], reverse = True)
    if len(x["connections"]) < k:
        x["connections"] = sortedByKeyList
    else:
        x["connections"] = sortedByKeyList[0:k]
    return(x)

#define the function for sorting aggregated recommendations and return top k per positition
def sortbyposition(x, k = 50):
    sortedByKeyList = sorted(x, key = lambda t: t["score"], reverse = True)
    if len(x) < k:
        x = sortedByKeyList
    else:
        x = sortedByKeyList[0:k]
    return(x)

#define the function for sorting by match score and get top k
def sortbyscore(contacts, k = 1000):
    sortedByKeyList = sorted(contacts, key = lambda t: t["matchScore"], reverse = True)
    if len(contacts) < k:
        contacts = sortedByKeyList
    else:
        contacts = sortedByKeyList[0:k]
    return(contacts)

#define the function for getting connected bounty users per each recommendation
def getconnections(x):
    connections = x.get("connections",[])
    for i, linkedInExternalId in enumerate(connections):
        bountyUsers = []
        for connection in db.LinkedInCollectionTest.find({"connections.linkedin.externalId":linkedInExternalId.get("linkedInExternalId",'')}):
            bountyUsers.append(connection.get('identity','').get('bountyUserId',''))
        x["connections"][i]["bountyUserConnections"] = bountyUsers
    return(x)

#define the function for removing duplicated recommendations per each position
def removedup(x):
    new_x = []
    for item in x:
        if item not in new_x:
            new_x.append(item)
    return(new_x)

#define the function for querying connections for specific users
def getcontacts(positionId,userId=None,degree = "first"):
    contacts = []
    for recommendation in db1.recommendations.find({"positionId":positionId}):
        contacts.append(recommendation.get("match"))
    return(contacts)

#define the function for preparing contacts before getting the training data
def cleancontacts(contacts):
    #define the experience and seniority maps
    experienceDict = {"less than 1 year":0,"1 to 2 years":1,"3 to 5 years":2,"6 to 10 years":3,"more than 10 years":4}
    seniorityDict = {"student/professor":0,"recruiter":0,"entry-level":1,"senior-level":2,"manager-level":3,"owner/C-level":4}
    #clean & get number of scored skills, experience & seniority and current job time (in month)
    for contact in contacts:
        contact["numScoredSkills"] = len(contact.get("scoredSkills"))
        contact["numExperience"] = experienceDict.get(contact.get("yearsOfExperience"))
        contact["numSeniority"] = seniorityDict.get(contact.get("seniority"))
        contact["currentJobMonth"] = float(contact.get("currentJobTime").split(" ")[0])
    return(contacts)

#define the function to get training data
def gettraining(data,XColumns,YColumn,dropNA=True,shuffle=True):
    #make a copy
    trainingData = DataFrame(data)
    #drop a few columns which we won't use for ML models
    trainingData = trainingData[XColumns+YColumn]
    if dropNA == True:
        #drop Na values if necessary
        trainingData = trainingData.dropna()
    if shuffle == True:
        #shuffle data if necessary
        trainingData = trainingData.reindex(np.random.permutation(trainingData.index))
    return(trainingData)

#define the function to normalize data SVM
def normalize(data):
    #normalize data by mean and standard deviation
    data_norm = (data - data.mean()) / max(data.std())
    #if the column is a constant, keep the original value
    ind = [i for i, x in enumerate(data.std()) if x == 0 or np.isnan(x)]
    if ind != []:
        data_norm.ix[:,ind] = (data - data.mean())
    #print data_norm
    return(data_norm)

#define the function to calculate the random forest CV error, k = 5 for default
def RFCV(data,YColumn,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200):
    #convert YColumn from a list to a string
    YColumn = YColumn[0]
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
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            testing = testingData.drop(YColumn,axis=1)
            #get model
            model = RandomForestRegressor(n_estimators=nTreeInitial,max_depth=d,max_features="sqrt")
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[YColumn])
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
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            testing = testingData.drop(YColumn,axis=1)
            #get model
            model = RandomForestRegressor(n_estimators=numTree,max_depth=bestDepth,max_features="sqrt")
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[YColumn])
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
def lassoCV(data,YColumn,k = 5,maxAlpha = 10):
    #convert YColumn from a list to a string
    YColumn = YColumn[0]
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
            #print testingData
            #print testingData
            #print trainingData
            #get test & training & target
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            testing = testingData.drop(YColumn,axis=1)
            #normalize data
            training = normalize(training)
            testing = normalize(testing)
            #print testing
            #get model
            model = linear_model.Lasso(alpha = a,normalize=True)
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[YColumn])
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
def ridgeCV(data,YColumn,k = 5,maxAlpha = 10):
    #convert YColumn from a list to a string
    YColumn = YColumn[0]
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
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            testing = testingData.drop(YColumn,axis=1)
            #normalize data
            training = normalize(training)
            testing = normalize(testing)
            #get model
            model = linear_model.Ridge(alpha = a,normalize=True)
            model = model.fit(training,target)
            #evaluate model and compute test error
            pred = np.array(model.predict(testing))
            testY = np.array(testingData[YColumn])
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
def linearCV(data,YColumn,k = 5):
    #convert YColumn from a list to a string
    YColumn = YColumn[0]
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
        training = trainingData.drop(YColumn,axis=1)
        target = trainingData[YColumn]
        testing = testingData.drop(YColumn,axis=1)
        #get model
        model = linear_model.LinearRegression()
        model = model.fit(training,target)
        #evaluate model and compute test error
        pred = np.array(model.predict(testing))
        testY = np.array(testingData[YColumn])
        CVtestMSE = CVtestMSE + np.linalg.norm(pred-testY)
    #get the CV error
    linearError = CVtestMSE
    print 'Test Mean Squared Error for linear regression:'
    print linearError
    return(["",linearError])

#define the function to fit the best model to all data to get ratings
def bestfit(allData,trainingData,model,parameter,YColumn):
    #convert YColumn from a list to a string
    YColumn = YColumn[0]
    #make a copy
    data = allData.copy()
    #fit the best model and make prediction
    #the possible model woule be "random forest", "lasso", "ridge", "linear regression"
    if model == "RF":
        #get parameters
        bestDepth = parameter[0]
        numTree = parameter[1]
        #get test & training & target
        training = trainingData.drop(YColumn,axis=1)
        target = trainingData[YColumn]
        #fit
        model = RandomForestRegressor(n_estimators=numTree,max_depth=bestDepth,max_features="sqrt")
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        allData[YColumn] = pred
    elif model == "lasso":
        #get parameters
        a = parameter
        #get test & training & target
        training = trainingData.drop(YColumn,axis=1)
        target = trainingData[YColumn]
        #normalize data
        training = normalize(training)
        data = normalize(data)
        #get model
        model = linear_model.Lasso(alpha = a,normalize=True)
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        allData[YColumn] = pred
    elif model == "ridge":
        #get parameters
        a = parameter
        #get test & training & target
        training = trainingData.drop(YColumn,axis=1)
        target = trainingData[YColumn]
        #normalize data
        training = normalize(training)
        data = normalize(data)
        #get model
        model = linear_model.Ridge(alpha = a,normalize=True)
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        allData[YColumn] = pred
    elif model == "linear":
        #get test & training & target
        training = trainingData.drop(YColumn,axis=1)
        target = trainingData[YColumn]
        #get model
        model = linear_model.LinearRegression()
        model = model.fit(training,target)
        #predict
        pred = model.predict(data)
        allData[YColumn] = pred
    else:
        raise ValueError("model not found")
    return(allData)


#retrieve data for all positions
print "connecting to mongodb from the local server"
client1 = MongoClient('localhost', 3001)
db1 = client1.meteor

#get inputs, position id and user id and validate those two inputs
positionId = '6iPyWnpCc9Wism3Eh'
userId = 'yjmqu8M2pLmja6bXF'
checkinputs(positionId,userId)

#select features and clean data
XColumns = ["numScoredSkills","numSeniority","expertiseLevel","currentJobMonth","numExperience","numConnections"]
YColumn = ["matchScore"]
contacts = cleancontacts(getcontacts(positionId))
data = gettraining(contacts,XColumns,YColumn)

#find the best random forest model by cross validation
print "getting the best random forest model"
[RFParameters,RFError] = RFCV(data,YColumn,k = 5,nTreeInitial = 50,maxDepth = 10,maxNumTrees = 200)
#find the best lasso model by cross validation

print "getting the best lasso model"
[lassoParameter,lassoError] = lassoCV(data,YColumn,k = 5,maxAlpha = 10)

#find the best rigde model by cross validation
print "getting the best ridge model"
[ridgeParameter,ridgeError] = ridgeCV(data,YColumn,k = 5,maxAlpha = 10)

#fit the simple linear model by cross validation
print "fitting the simple linear model"
[linearParameter,linearError] = linearCV(data,YColumn,k = 5)

#print the error for each model and get the best model with smallest CV error
modelTypes = ["RF","lasso","ridge","linear"]
modelParameters = [RFParameters,lassoParameter,ridgeParameter,linearParameter]
modelErrors = [RFError,lassoError,ridgeError,linearError]
bestModel = modelTypes[modelErrors.index(min(modelErrors))]
bestParameter = modelParameters[modelErrors.index(min(modelErrors))]
print "the smallest error for each model:"
print "random forest:"+str(RFError)+", lasso:"+str(lassoError)+", ridge:"+str(ridgeError)+", linear:"+str(linearError)

#fit the best model to all emails in the central email list
print "working on final fit using the best selected model, which is " + bestModel
#for unseen training data, we don't need to have the y variable
allData = gettraining(contacts,XColumns,YColumn=[],dropNA=False,shuffle=False)
dataOutput = bestfit(allData,data,model = bestModel,parameter = bestParameter,YColumn=YColumn)
#update the rank based on the predicted response ratings
print data.head(10)
print dataOutput.head(10)

#get the rank updated to the mongodb
print "working on updating the rank to mongodb"
updatedEmailList = pd.concat([centralEmailList, dataOutput[["responseRating"]]], axis=1)
