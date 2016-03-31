#this script generates a proposed beer box for one specific beer club member
#we need to input the propose box id
#recommended beers are based on region, taste, strength, type of the user
#and price, remaining, popularity of beers
#The input collections: "beers","rawSalesData","inventory","locationReference","beerBox" and "proposeBox"
#The outputs will be add into "beerBox" and "proposeBox"


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

#define the function to get the beer region
def getbeerregion(beersData,locationList,regionList=["eastCoast","westCoast","midWest","europe"]):
    #get category list for special location
    beerRegionList = []
    for item in beersData:
        #get and clean the location
        beerRawLocation = x.iloc[i]['location']
        beerLocation = beerRawLocation.split(",")[-1]
        #see whether it is the US
        if " USA" in beerLocation:
            #see whether it is eastCoast
            eastCoastRegions = locationList.get("eastCoast")
            for eastCoastRegion in eastCoastRegions:
                if eastCoastRegion in beerLocation:
                    beerRegionList.append("eastCoast")
                    break
            #see whether it is westCoast
            westCoastRegions = locationList.get("westCoast")
            for westCoastRegion in westCoastRegions:
                if westCoastRegion in beerLocation:
                    beerRegionList.append("westCoast")
                    break
            #see whether it is midWest
            beerRegionList.append("midWest")
        else:
            #see whether it is europe
            europeanRegions = locationList.get("europe")
            for europeanRegion in europeanRegions:
                if europeanRegion in beerLocation:
                    beerRegionList.append("europe")
                    break
            #see whether it is other region
            beerRegionList.append("others")
    beersData["region"] = beerRegionList
    return(beersData)

#define the function to get box user info
def getboxusers(proposeBoxId):
    boxUsersInfoList = []
    for proposeBoxUser in db1.proposeBox.find({"_id":proposeBoxId}):
        boxUserDict = {}
        boxUserDict["proposeBoxId"] = proposeBoxUser.get("_id")
        boxUserDict["userId"] = proposeBoxUser.get("userId")
        boxUserDict["type"] = proposeBoxUser.get("type")
        #get user strength preferences, such as light, medium and strong
        boxUserDict["strengthLight"] = proposeBoxUser.get("preferences",{}).get("strength",{}).get("light")
        boxUserDict["strengthMedium"] = proposeBoxUser.get("preferences",{}).get("strength",{}).get("medium")
        boxUserDict["strengthStrong"] = proposeBoxUser.get("preferences",{}).get("strength",{}).get("strong")
        #get user taste preferences, such as stout, IPA, porter, sour
        boxUserDict["tasteStout"] = proposeBoxUser.get("preferences",{}).get("taste",{}).get("stout")
        boxUserDict["tasteIPA"] = proposeBoxUser.get("preferences",{}).get("taste",{}).get("ipa")
        boxUserDict["tastePorter"] = proposeBoxUser.get("preferences",{}).get("taste",{}).get("porter")
        boxUserDict["tasteSour"] = proposeBoxUser.get("preferences",{}).get("taste",{}).get("sour")
        #get user region preferences, such as west coast, east coast, mid west and europe
        boxUserDict["regionWestCoast"] = proposeBoxUser.get("preferences",{}).get("region",{}).get("westCoast")
        boxUserDict["regionEastCoast"] = proposeBoxUser.get("preferences",{}).get("region",{}).get("eastCoast")
        boxUserDict["regionMidWest"] = proposeBoxUser.get("preferences",{}).get("region",{}).get("midWest")
        boxUserDict["regionEurope"] = proposeBoxUser.get("preferences",{}).get("region",{}).get("europe")
        #set up some other attributes
        boxUserDict["initialValue"] = 130 - 30*(["exclusive","deluxe","basic"].index(boxUserDict.get("type")))
        boxUserDict["value"] = random.random_integers(0,4) * 5
        boxUserDict["freedom"] = 5 - 2*(["exclusive","deluxe","basic"].index(boxUserDict.get("type")))
        boxUserDict["shippingCost"] = 30
        boxUserDict["packagingCost"] = 10
        boxUsersInfoList.append(boxUserDict)
    return(boxUsersInfoList)

#define the function to check all data inputs
def checkinputs(inventoryInfo,beersData,users,proposeBoxId):
    #check proposeBoxId
    if db1.proposeBox.find_one({"_id":proposeBoxId}) == None:
        raise ValueError("the propose box doesn't exit")
    #check inventoryId
    if len(inventoryInfo) < 1:
        raise ValueError("failed to load any inventories")
    #check beers data
    if beersData.shape[0] < 1:
        raise ValueError("failed to merge inventory data with sales data")
    #check box users data
    if len(users) < 1:
        raise ValueError("failed to load any propose box which needs to be machine learned")
    #print beers data info
    print str(beersData.shape[0]) + " beers loaded"
    #print users info
    print str(len(users)) + " propose box users loaded, and the detailed info is shown below:"
    print users

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

#define the function to get initial recommended beers
def getproposebeers(users,beersData,minRemaining=10,total=6,profitRange=[-20,100],top=100):
    #make a copy of beers data
    beers = beersData.copy()
    #pre-filter beers by remaining, status and rating
    beers = beers.loc[beers["remaining"] >= minRemaining]
    beers = beers.loc[beers["status"] == "available"]
    #get recommendation
    recommendation = []
    #get user info
    for i,user in enumerate(users):
        #get user info
        userId = user.get("userId")
        freedom = user.get("freedom")
        userType = user.get("type")
        #get user strength preferences
        strengthLight = user.get("strengthLight")
        strengthMedium = user.get("strengthMedium")
        strengthStrong = user.get("strengthStrong")
        #get user taste preferences
        tasteStout = user.get("tasteStout")
        tasteIPA = user.get("tasteIPA")
        tastePorter = user.get("tastePorter")
        tasteSour = user.get("tasteSour")
        #get user region preferences
        regionWestCoast = user.get("regionWestCoast")
        regionEastCoast = user.get("regionEastCoast")
        regionMidWest = user.get("regionMidWest")
        regionEurope = user.get("regionEurope")
        #set up the profit range
        minProfit = profitRange[0]
        maxProfit = profitRange[1]
        #filter by strength and taste (not finished)
        #if ... then ...
        #get budget
        budget = user.get("initialValue") - user.get("shippingCost") - user.get("packagingCost")
        #set initial values
        totalPrice = 0
        counter = 0
        unique = False
        #recommend beers only if they are in the profit range, and also unique
        while (budget - totalPrice < minProfit or budget - totalPrice > maxProfit or unique == False):
            counter += 1
            goodBeers = beers.sort_index(by=['totalSales','currentPrice','remaining'],ascending=[False,True,False])
            goodBeers = goodBeers.iloc[random.choice(range(top),freedom,False)]
            badBeers = beers.sort_index(by=['currentPrice','totalSales','remaining'],ascending=[True,False,False])
            badBeers = badBeers.iloc[random.choice(range(top),total-freedom,False)]
            recommendedBeers = goodBeers["beerId"].tolist()+badBeers["beerId"].tolist()
            #see whether satisfy the conditions to proceed
            if len(set(recommendedBeers)) == len(recommendedBeers):
                unique = True
            totalPrice = sum(goodBeers["currentPrice"].tolist()+badBeers["currentPrice"].tolist())
            print "generating " + str(total) + " beers with total price " + str(totalPrice) + " for " + str(userType) + " propose box user '" + str(userId) + "' for attempt " + str(counter)
            #stop the process if too many attempts are made
            if counter > 100:
                raise ValueError("too many attempts, could not fulfill the price requirement, please try again")
        inventoryIds = getinventoryid(user,recommendedBeers)
        proposeBeersDict = {}
        proposeBeersDict["proposeBoxId"] = user.get("proposeBoxId")
        proposeBeersDict["userId"] = userId
        proposeBeersDict["type"] = userType
        proposeBeersDict["recommendedBeers"] = recommendedBeers
        proposeBeersDict["inventoryIds"] = inventoryIds
        recommendation.append(proposeBeersDict)
    return(recommendation)

#define the function to get inventory id
def getinventoryid(user,recommendedBeers):
    inventoryIdList = []
    for beerId in recommendedBeers:
        #for some beers having multiple styles (both can and bottle), choose the cheapest one as the default
        inventoryId = list(db1.inventory.aggregate([ {"$match":{"beerId":beerId}}, {"$sort":{"price.currentPrice":1}},{"$limit":1},{"$project":{"_id":1}}]))
        inventoryIdList.append(str(inventoryId[0].get("_id")))
    return(inventoryIdList)

#insert into mongodb
def insertdb(proposeBeers):
    #insert proposed beers box to the beerBox collection
    print "inserting recommended beers to the beerBox collection"
    for proposeBeer in proposeBeers:
        print proposeBeer
        userId = proposeBeer.get("userId")
        proposeBoxId = proposeBeer.get("proposeBoxId")
        beers = proposeBeer.get("recommendedBeers")
        inventoryIds = proposeBeer.get("inventoryIds")
        userType = proposeBeer.get("type")
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
        db1.proposeBox.update({"_id":proposeBoxId},{'$set':{"beerBoxId":beerBoxId,"generateType":"machine","toMachineLearn":False,"machineLearnedAt":datetime.utcnow(),"beers":beers,"inventoryIds":inventoryIds}})
    print "done"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    # client1 = MongoClient('localhost', 27017)
    # db1 = client1.appDB

    #define a propose box id (for debug only)
    #proposeBoxId = "tNEfnuaKfMM8mA6XF"

    #get all inventory and location info
    print "getting inventory, location and sales info"
    inventoryInfo = getinventory()
    locationReference = getlocationreference()
    salesData = cleansalesdata(getsalesdata())
    #add sales data to inventory data
    beersData = addsalesdata(inventoryInfo,salesData)
    #get users info
    users = getboxusers(proposeBoxId)
    #check all inputs
    checkinputs(inventoryInfo,beersData,users,proposeBoxId)
    #get initial recommended beers
    print "working on getting propose beer box"
    proposeBeers = getproposebeers(users,beersData,minRemaining=10,total=6,profitRange=[-20,100],top=100)
    #insert proposed beer boxes
    insertdb(proposeBeers)
