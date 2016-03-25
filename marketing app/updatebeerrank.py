#this script updates the rank in the central email list
#this script should be executed before generating the propose email list,
#since the propose email list is based on the ranks in the central email list

#note that we first retrieve all sent emails as traning data. then find the
#the best machine learning model and use it to predict the response rating,
#and updated the rank based one the rating, (high to low)

#todo:
#1: include more freatures


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime
import collections
import pandas as pd
import numpy as np
import csv
from sklearn.ensemble import RandomForestRegressor
from sklearn import linear_model
from sklearn import svm


#define the function to check the feedback (response)
def checkresponse(sentEmailList,minValidResponse):
    response = sentEmailList["response"].values.tolist()
    responseCounter = collections.Counter(response)
    validResponseCounter = 0
    for responseKey in responseCounter.keys():
        print str(responseKey) + " : " + str(responseCounter[responseKey])
        #calculate for # of valid response
        if responseKey != "no":
            validResponseCounter += responseCounter[responseKey]
    if validResponseCounter < minValidResponse:
        raise ValueError("not enough data, the number of valid responses is too small")

#define the function to get the response for each sent email
def getresponserating(sentEmailList,ratingParameters):
    responseRatingList = []
    for response in sentEmailList['response']:
        responseRating = ratingParameters.get(response)
        responseRatingList.append(responseRating)
    sentEmailList["responseRating"] = responseRatingList
    return(sentEmailList)

#define the function to get the waiting day (# of days from last order)
def getwaitingday(sentEmailList):
    #get today's date
    dateFormat = "%m/%d/%y"
    today = datetime.now().strftime(dateFormat)
    #print today
    waitingDayList = []
    for lastOrderDate in sentEmailList["lastOrderDate"]:
        dateDiff = datetime.strptime(today,dateFormat) - datetime.strptime(lastOrderDate,dateFormat)
        waitingDayList.append(dateDiff.days)
    sentEmailList["waitingDay"] = waitingDayList
    return(sentEmailList)

#define the function to get training data
def gettraining(data,XColumns,dummyColumns,YColumn = [],dropNA = True,shuffle = True):
    #make a copy
    trainingData = data.copy()
    #drop a few columns which we won't use for ML models
    trainingData = trainingData[XColumns+YColumn]
    if dropNA == True:
        #drop Na values if necessary
        trainingData = trainingData.dropna()
    if shuffle == True:
        #shuffle data if necessary
        trainingData = trainingData.reindex(np.random.permutation(trainingData.index))
    print "the training data has " + str(trainingData.shape[0]) + " rows and " + str(trainingData.shape[1]) + " columns"
    #convert categorical data to dummy variables
    if dummyColumns != []:
        trainingData = getdummy(trainingData,dummyColumns,stage = "training")
    return(trainingData)

#define the function to convert categorical data to dummy variables
def getdummy(rawData,categories,stage):
    #make a copy
    data = rawData.copy()
    if stage == "training":
        for category in categories:
            columns = list(data.columns.values)
            #print data[category]
            columnValues = set(data[category])
            #print columnValues
            dummy = pd.get_dummies(data[category],prefix=category)
            #print dummy.head(10)
            if dummy.shape[1] > 1:
                columns.remove(category)
                data = data[columns].join(dummy.ix[:,1:])
            elif dummy.shape[1] == 1:
                columns.remove(category)
                data = data[columns].join(dummy)
    if stage == "testing":
        #print categories
        columns = list(data.columns.values)
        for category in categories:
            columnValues = set(data[category])
            #print columnValues
            dummy = pd.get_dummies(data[category],prefix=category)
            #print dummy.head(10)
            dummyColumns = list(dummy.columns.values)
            for dummyColumn in dummyColumns:
                if dummyColumn in columns:
                    data[dummyColumn] = dummy[dummyColumn]
            columns.remove(category)
        data = data[columns]
            #print dummy.head(10)
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
            #get test & training & target
            training = trainingData.drop(YColumn,axis=1)
            target = trainingData[YColumn]
            testing = testingData.drop(YColumn,axis=1)
            #normalize data
            training = normalize(training)
            testing = normalize(testing)
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

#define the function to get the updated rank for all emails in the central email list
def updaterank(centralEmailList,dataOutput,YColumn):
    #data = pd.concat([centralEmailList, dataOutput], axis=1)
    YColumn = YColumn[0]
    #print dataOutput.head(10)
    #update the column 'rating' by predicted values
    #dataOutput = dataOutput.sort_index(by=[YColumn],ascending=[False])
    #newRating = dataOutput[YColumn]
    #join updated rating
    updatedEmailList = pd.concat([centralEmailList, dataOutput[["responseRating"]]], axis=1)
    if updatedEmailList["responseRating"].isnull().any():
        raise ValueError("we have missed some ratings, which is not expected")
    if updatedEmailList.shape[0] != centralEmailList.shape[0]:
        raise ValueError("we have some missing values, which is not expected")
    #print updatedEmailList.head(10)
    updatedEmailList = updatedEmailList.sort([YColumn],ascending=[False])
    #print updatedEmailList.head(5)
    return(updatedEmailList)

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

#define the email rating paramters
ratingParameters = {"no":0,"open":1,"click":2,"sold":3}
minValidResponse = 10

#load the sent email list (the most recent) from cassandra
print "retrieving the most recent sent email list as training data from cassandra"
rawEmailList = session.execute("""
select * from "sentEmailList"
""")

#convert paged results to a list then a dataframe
sentEmailList = pd.DataFrame(list(rawEmailList))
#pre-check and summarize all responses
checkresponse(sentEmailList,minValidResponse)

#calculate the rank for each email (user)
print "preparing the training data"
sentEmailList = getresponserating(sentEmailList,ratingParameters)
sentEmailList = getwaitingday(sentEmailList)

print sentEmailList.head(10)

aaa
#load the sent email list (the most recent) from cassandra
print "retrieving all emails in the central email list as test data from cassandra"
rawEmailList = session.execute("""
select * from "centralEmailList"
""")

#convert paged results to a list then a dataframe
centralEmailList = pd.DataFrame(list(rawEmailList))

#calculate the rank for each email (user)
print "preparing the test data from the central email list with " + str(centralEmailList.shape[0]) + " emails"
centralEmailList = getwaitingday(centralEmailList)

#get shuffled training data, selected a few columns which will be considered
print "getting training data"
#we will add more columns later
#XColumns = ["gender","waitingDay","orderCount","preferences","quantityCount","age","shippingProvince","type"]
XColumns = ["waitingDay","totalSales","orderCount","quantityCount","type"]
YColumn = ["responseRating"]
dummyColumns = ["type"]
data = gettraining(sentEmailList,XColumns,dummyColumns,YColumn,dropNA = True,shuffle = True)

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
allData = gettraining(centralEmailList,XColumns,dummyColumns,dropNA=False,shuffle=False)
dataOutput = bestfit(allData,data,model = bestModel,parameter = bestParameter,YColumn=YColumn)
#update the rank based on the predicted response ratings
updatedEmailList = updaterank(centralEmailList,dataOutput,YColumn)

#update all users in the central email list for new ranks
print "update the rank in 'centralEmailList' to cassandra, please wait about 1 minute"
n = updatedEmailList.shape[0]
for i in range(n):
    #rank is based on sorted predicted rating. which is 'responseRating'
    rank = i + 1
    email = updatedEmailList.iloc[i]['email']
    prepared_stmt = session.prepare ("""
    UPDATE "centralEmailList" SET rank = ? WHERE email = ?
    """)
    bound_stmt = prepared_stmt.bind([rank,email])
    stmt = session.execute(bound_stmt)
print str(n) + " rows of data have been updated"
