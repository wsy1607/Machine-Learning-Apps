#this script edits one shipped beer box
#The inputs are collections "beers","rawSalesData","inventory","locationReference","beerBox" and "shippedBox"
#The outputs will be add into "beerBox" and "shippedBox"

#inputs are shown below:
#shippedBoxId: a string of the box id of the propose box which needs to be edited
#toKeepInventoryIds: a list of beers to be kept, can be empty
#toAddInventoryIds: a list of beers to be added, can be empty
#toRemoveInventoryIds: a list of beers to be removed, can be empty
#boxOptions: a dictionary (or a key-value pair like a json)
#boxOptions.price: 1: increase, 0:keep, -1: decrease
#boxOptions.weight: 1: increase, 0:keep, -1: decrease
#boxOptions.rareness: 1: increase, 0:keep, -1: decrease

#for different combinations of the inputs, we generally define four functional actions.
#1, swap: if the user want to add and remove the same number of beers
#2, auto: if the user don't specify any beer to be added or removed
#3, add: if the user specify the beers to be removed (we need to decide which beers to be added)
#4, remove: if the user specify the beers to be added (we need to decide which beers to be removed)

#the basic process is shown as below:
#1: get and validate the user inputs
#2: get real-time beer data and merge with sales data
#3: try to keep liked beers if there is enough space
#4: determine the action, and edit beer box by box options

#to do: add container info for weight calculation


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
from bson.objectid import ObjectId
import sys


#define the function to check all inputs
def checkallinputs(shippedBoxId,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,nBeers):
    #check the input, shippedBoxId
    shippedBox = db1.shippedBox.find_one({'_id':shippedBoxId})
    if shippedBox == None:
        raise ValueError("cannot find the shipped box")

    #check the input, toKeepInventoryIds, toAddInventoryIds and toRemoveInventoryIds
    if (type(toKeepInventoryIds) is list) == False:
        raise ValueError("beers should be in a list")
    if (type(toAddInventoryIds) is list) == False:
        raise ValueError("beers should be in a list")
    if (type(toRemoveInventoryIds) is list) == False:
        raise ValueError("beers should be in a list")
    if len(toKeepInventoryIds) >= nBeers:
        raise ValueError("trying to keep too many beers")
    if len(toAddInventoryIds) > nBeers - len(toKeepInventoryIds):
        raise ValueError("trying to add too many beers")
    if len(toRemoveInventoryIds) > nBeers - len(toKeepInventoryIds):
        raise ValueError("trying to remove too many beers")
    if toAddInventoryIds != []:
        for toAddInventoryId in toAddInventoryIds:
            toAddInventory = db1.inventory.find_one({'_id':toAddInventoryId})
            if toAddInventory == None:
                raise ValueError("trying to add an inventory '" + str(toAddInventoryId) + "', which doesn't exist")
    if toAddInventoryIds != []:
        for toAddInventoryId in toAddInventoryIds:
            if toAddInventoryId in shippedBox.get("inventoryIds"):
                raise ValueError("trying to add one inventory '" + str(toAddInventoryId) + "', which is already in this shipped box")
    if toKeepInventoryIds != []:
        for toKeepInventoryId in toKeepInventoryIds:
            if toKeepInventoryId not in shippedBox.get("inventoryIds"):
                raise ValueError("trying to keey one inventory '" + str(toKeepInventoryId) + "', which is not in this shipped box")
    if toRemoveInventoryIds != []:
        for toRemoveInventoryId in toRemoveInventoryIds:
            if toRemoveInventoryId not in shippedBox.get("inventoryIds"):
                raise ValueError("trying to remove one inventory '" + str(toRemoveInventoryId) + "', which is not in this shipped box")
    if len(set(toKeepInventoryIds)) != len(toKeepInventoryIds):
        raise ValueError("the input toKeepInventoryIds are not unique")
    if len(set(toAddInventoryIds)) != len(toAddInventoryIds):
        raise ValueError("the input toAddInventoryIds are not unique")
    if len(set(toRemoveInventoryIds)) != len(toRemoveInventoryIds):
        raise ValueError("the input toRemoveInventoryIds are not unique")
    if len(set(toRemoveInventoryIds+toKeepInventoryIds)) != len(toRemoveInventoryIds)+len(toKeepInventoryIds):
        raise ValueError("trying to keep and remove the same beer")

    #check the input, boxOptions
    priceOption = boxOptions.get("price")
    weightOption = boxOptions.get("weight")
    remainingOption = boxOptions.get("rareness")
    if priceOption not in [-1,0,1] or weightOption not in [-1,0,1] or remainingOption not in [-1,0,1]:
        raise ValueError("the input 'boxOptions' is wrong")
    else:
        print "all inputs are right"

    #confirm the action
    if priceOption == -1:
        print "we will decrease prices for the shippedBox '" + str(shippedBoxId)
    elif priceOption == 1:
        print "we will increase prices for the shippedBox '" + str(shippedBoxId)
    if weightOption == -1:
        print "we will decrease weights for the shippedBox '" + str(shippedBoxId)
    elif weightOption == 1:
        print "we will increase weights for the shippedBox '" + str(shippedBoxId)
    if remainingOption == -1:
        print "we will decrease rareness for the shippedBox '" + str(shippedBoxId)
    elif remainingOption == 1:
        print "we will increase rareness for the shippedBox '" + str(shippedBoxId)
    if toKeepInventoryIds != []:
        print "we will keep " + str(len(toKeepInventoryIds)) + " beers, which are '" + "', '".join(toKeepInventoryIds) + "'"
    if toAddInventoryIds != []:
        print "we will add " + str(len(toAddInventoryIds)) + " beers, which are '" + "', '".join(toAddInventoryIds) + "'"
    if toRemoveInventoryIds != []:
        print "we will remove " + str(len(toRemoveInventoryIds)) + " beers, which are '" + "', '".join(toRemoveInventoryIds) + "'"

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
def getshippedboxuser(shippedBoxId):
    shippedBoxUser = db1.shippedBox.find_one({"_id":shippedBoxId})
    boxUserDict = {}
    boxUserDict["shippedBoxId"] = shippedBoxUser.get("_id")
    boxUserDict["userId"] = shippedBoxUser.get("userId")
    #boxUserDict["type"] = shippedBoxUser.get("type")
    #get user strength preferences, such as light, medium and strong
    boxUserDict["strengthLight"] = shippedBoxUser.get("preferences",{}).get("strength",{}).get("light")
    boxUserDict["strengthMedium"] = shippedBoxUser.get("preferences",{}).get("strength",{}).get("medium")
    boxUserDict["strengthStrong"] = shippedBoxUser.get("preferences",{}).get("strength",{}).get("strong")
    #get user taste preferences, such as stout, IPA, porter, sour
    boxUserDict["tasteStout"] = shippedBoxUser.get("preferences",{}).get("taste",{}).get("stout")
    boxUserDict["tasteIPA"] = shippedBoxUser.get("preferences",{}).get("taste",{}).get("ipa")
    boxUserDict["tastePorter"] = shippedBoxUser.get("preferences",{}).get("taste",{}).get("porter")
    boxUserDict["tasteSour"] = shippedBoxUser.get("preferences",{}).get("taste",{}).get("sour")
    #get user region preferences, such as west coast, east coast, mid west and europe
    boxUserDict["regionWestCoast"] = shippedBoxUser.get("preferences",{}).get("region",{}).get("westCoast")
    boxUserDict["regionEastCoast"] = shippedBoxUser.get("preferences",{}).get("region",{}).get("eastCoast")
    boxUserDict["regionMidWest"] = shippedBoxUser.get("preferences",{}).get("region",{}).get("midWest")
    boxUserDict["regionEurope"] = shippedBoxUser.get("preferences",{}).get("region",{}).get("europe")
    #get shipped box beers
    boxUserDict["beerIds"] = shippedBoxUser.get("beers")
    boxUserDict["inventoryIds"] = shippedBoxUser.get("inventoryIds")
    #get liked beers & user type for this user by looking at the corresponding propose box
    proposeBoxUser = db1.proposeBox.find_one({"shippedBoxId":shippedBoxId})
    boxUserDict["likedBeers"] =proposeBoxUser.get("likedBeers")
    boxUserDict["type"] = proposeBoxUser.get("type")
    #get shipped box user info
    boxUserDict["initialValue"] = 130 - 30*(["exclusive","deluxe","basic"].index(boxUserDict.get("type")))
    boxUserDict["value"] = random.random_integers(0,4) * 5
    boxUserDict["freedom"] = 4 - 2*(["exclusive","deluxe","basic"].index(boxUserDict.get("type")))
    boxUserDict["shippingCost"] = 30
    return(boxUserDict)

#define the function to add liked beer the 'keep' list
def addtokeep(shippedBoxUser,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,nBeers):
    likedBeers = shippedBoxUser.get("likedBeers")
    if likedBeers != []:
        #get liked beers inventory id
        for likedBeerId in likedBeers:
            for oldBoxInventoryId in shippedBoxUser.get("inventoryIds"):
                #keep this liked beer if this beer isn't in user-input lists and we still have space in the 'keep' list
                if (likedBeerId in oldBoxInventoryId) and (oldBoxInventoryId not in toKeepInventoryIds+toAddInventoryIds+toRemoveInventoryIds):
                    if (len(toAddInventoryIds) < nBeers - 1 - len(toKeepInventoryIds)) and (len(toRemoveInventoryIds) < nBeers - 1 - len(toKeepInventoryIds)):
                        #append this beer to the 'keep' list
                        print "adding the liked beer '" + oldBoxInventoryId + "' to the 'keep' list"
                        toKeepInventoryIds.append(oldBoxInventoryId)
    return(toKeepInventoryIds)

#define the function to get sales data from mongodb
def getsalesdata():
    sales = []
    #retrieve raw sales data from mongodb, then convert to a dataframe
    for transaction in db1.rawSalesData.find():
        sales.append(transaction)
    sales = DataFrame(sales)
    #create a column for unique city
    sales["shippingAddress"] = sales["shippingCity"] + " " + sales["shippingProvince"]
    return(sales)

#define the function to clean sales data which will only keep sales for beers
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
    #output as a dictionary
    salesDataDict = salesData.to_dict().get(value)
    return(salesDataDict)

#define the function to merge sales to inventory data
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

#define the function to calculate the beer weight based on ml
def getweight(beersData):
    #make a copy
    beers = beersData.copy()
    #calculate weight based on ml for each beer
    weightList = []
    for size in beers["size"]:
        weightML = getweightbysize(size)
        weightList.append(weightML)
    beers["weightInMl"] = weightList
    return(beers)

#define the function to convert oz or L to ml
def getweightbysize(size):
    if "mL" in size:
        weightML = round(float(size.split('mL')[0])*1)
    elif "oz" in size:
        weightML = round(float(size.split('oz')[0])*29.5735)
    elif "L" in size:
        weightML = round(float(size.split('L')[0])*1000)
    else:
        weightML = round(0)
        print "warning: an unknown size has been found"
    return(weightML)

#define the function to get the action
def geteditaction(toAddInventoryIds,toRemoveInventoryIds):
    #if user doesn't specify anything, action would be 'auto'
    if toAddInventoryIds == [] and toRemoveInventoryIds == []:
        editAction = 'auto'
    #if user would like to add and remove the same number of beers, action would be 'swap'
    elif len(toAddInventoryIds) == len(toRemoveInventoryIds):
        editAction = 'swap'
    #if user inputs more beers to be added, action would be 'remove' meaning that we have to remove beers
    elif len(toAddInventoryIds) > len(toRemoveInventoryIds):
        editAction = 'remove'
    #if user inputs more beers to be removed, action would be 'add' meaning that we have to add beers
    elif len(toAddInventoryIds) < len(toRemoveInventoryIds):
        editAction = 'add'
    #check action
    else:
        raise ValueError("action is not clear when adding " + str(len(toAddInventoryIds)) + "beers and removing " + str(len(toRemoveInventoryIds)) + "beers")
    return(editAction)

#define the function to execute the action
def executeAction(shippedBoxUser,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,editAction,beersData,nBeers,priceUnit,priceVar,weightUnit,weightVar,remainingUnit,remainingVar):
    #step one: get old shipped box inventory ids
    oldBoxInventoryIds = shippedBoxUser.get("inventoryIds")
    #step two: get removed beers if necessary
    toRemoveInventoryIds = edittoremoveinventory(oldBoxInventoryIds,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,editAction,nBeers)
    #step three: get added beers and also get the new shipped box
    newBoxInfo = edittoaddinventory(shippedBoxUser,oldBoxInventoryIds,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,editAction,beersData,nBeers,priceUnit,priceVar,weightUnit,weightVar,remainingUnit,remainingVar)
    return(newBoxInfo)

#define the function to get removed beers
def edittoremoveinventory(oldBoxInventoryIds,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,editAction,nBeers):
    #if the action is 'remove', the number of beers to be removed would be the differnce
    if editAction == "remove":
        n = len(toAddInventoryIds) - len(toRemoveInventoryIds)
    #if the action is 'auto', the number of beers to be removed would be half of the number in beer box exclude ones to keep
    if editAction == "auto":
        n = (1 + nBeers - len(toKeepInventoryIds))/2
    #we need to create a list of beers to be removed only if the action is 'remove' or 'auto'
    #if the action is 'add' or 'swap', we will skip this part since there's no need to do anything
    if editAction == "remove" or editAction == "auto":
        #get ids which will be considered to get removed excluding beers which user want to keep or remove
        candidateInventoryIds = [inventoryId for inventoryId in oldBoxInventoryIds if inventoryId not in (toKeepInventoryIds+toRemoveInventoryIds)]
        #get the rank (based on price, weight and rareness) for each beer, then remove n beers having the n lowest ranks
        candidateInventoryRanks = getrank(candidateInventoryIds,boxOptions)
        #if we get tied, we will choose randomly using 'count' variable
        count = 0
        for i in range(len(candidateInventoryIds)):
            if candidateInventoryRanks[i] in sorted(candidateInventoryRanks)[:n]:
                #append to the 'to remove' list
                toRemoveInventoryIds.append(candidateInventoryIds[i])
                count += 1
            if count == n:
                break
    return(toRemoveInventoryIds)

#define the function to get beer rank which will help to decide which beer to be removed
def getrank(inventoryIds,boxOptions):
    #get prices, weight and remaining
    inventoryPrices = []
    inventoryWeights = []
    inventoryRemainings = []
    for inventoryId in inventoryIds:
        inventory = db1.inventory.find_one({"_id":inventoryId})
        inventoryPrices.append(inventory.get("price").get("currentPrice"))
        inventoryWeights.append(getweightbysize(inventory.get("type").get("size")))
        inventoryRemainings.append(inventory.get("remaining"))
    #get box options
    priceOption = boxOptions.get("price")
    weightOption = boxOptions.get("weight")
    remainingOption = boxOptions.get("rareness")
    #get ranks based on boxOptions,
    #reverse the rareness rank since "-1" means increasing the remaining level
    inventoryRanks = []
    for i in range(len(inventoryIds)):
        priceRank = sorted(inventoryPrices).index(inventoryPrices[i])
        weightRank = sorted(inventoryWeights).index(inventoryWeights[i])
        rarenessRank = sorted(inventoryRemainings,reverse=True).index(inventoryRemainings[i])
        overallRank = priceRank * priceOption + weightRank * weightOption + rarenessRank * remainingOption
        inventoryRanks.append(overallRank)
    return(inventoryRanks)

#define the function to get added beers
def edittoaddinventory(shippedBoxUser,oldBoxInventoryIds,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,editAction,beersData,nBeers,priceUnit,priceVar,weightUnit,weightVar,remainingUnit,remainingVar):
    #we will add some new recommended beers, only if action is 'add' or 'auto',
    if editAction in ["add","auto"]:
        #get price, weight and remaining info for removed beers as the baseline
        toRemoveInventoryPrices = []
        toRemoveInventoryWeights = []
        toRemoveInventoryRemainings = []
        for inventoryId in toRemoveInventoryIds:
            inventory = db1.inventory.find_one({"_id":inventoryId})
            toRemoveInventoryPrices.append(inventory.get("price").get("currentPrice"))
            toRemoveInventoryWeights.append(getweightbysize(inventory.get("type").get("size")))
            toRemoveInventoryRemainings.append(inventory.get("remaining"))
        #get price, weight and rareness options
        priceOption = boxOptions.get("price")
        weightOption = boxOptions.get("weight")
        remainingOption = boxOptions.get("rareness")
        #get price, weight and rareness ranges for beers to be added,
        #the calculation is based on option value unit and option value variant
        toAddPriceRange = getrange(toRemoveInventoryPrices,priceOption,priceUnit,priceVar)
        toAddWeightRange = getrange(toRemoveInventoryWeights,weightOption,weightUnit,weightVar)
        toAddRemainingRange = getrange(toRemoveInventoryRemainings,remainingOption,remainingUnit,remainingVar)
        #based on those value ranges, we generate recommended beers for this box user, returning both inventoryId and beerId
        newBoxInfo = addrecommendedbeers(shippedBoxUser,oldBoxInventoryIds,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,beersData,nBeers,toAddPriceRange,toAddWeightRange,toAddRemainingRange)
    #if action is 'swap' or 'remove', we don't need to generate any new recommended beers,
    #we just need to directy get both inventory id and beer id for those user-defined 'toAddBeers'
    if editAction in ["swap","remove"]:
        newBoxBeerIds = []
        newBoxInventoryIds = []
        #first, get all ids for beers in the old shipped box which will be kept
        #oldBoxInventoryIds = shippedBoxUser.get("inventoryIds")
        for oldBoxInventoryId in oldBoxInventoryIds:
            if oldBoxInventoryId not in toRemoveInventoryIds:
                oldBoxBeerId = db1.inventory.find_one({"_id":oldBoxInventoryId}).get("beerId")
                newBoxBeerIds.append(oldBoxBeerId)
                newBoxInventoryIds.append(oldBoxInventoryId)
        #second, append all ids for user-defined 'toAddBeers'
        for toAddInventoryId in toAddInventoryIds:
            toAddBeerId = db1.inventory.find_one({"_id":toAddInventoryId}).get("beerId")
            newBoxBeerIds.append(toAddBeerId)
            newBoxInventoryIds.append(toAddInventoryId)
        newBoxInfo = {"newBoxInventoryIds":newBoxInventoryIds,"newBoxBeerIds":newBoxBeerIds}
    return(newBoxInfo)

#define the function to get numeric range for each box option
def getrange(inventoryValues,valueOption,valueUnit,valueVar):
    #the range formula: [average +- unit - var, average +- unit + var],
    #+- unit is based on the option,
    inventoryAverageValue = np.mean(inventoryValues)
    minRange = inventoryAverageValue + valueOption * valueUnit - valueVar
    maxRange = inventoryAverageValue + valueOption * valueUnit + valueVar
    return([minRange,maxRange])

#define the function to get initial recommended beers
def addrecommendedbeers(shippedBoxUser,oldBoxInventoryIds,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,beersData,nBeers,toAddPriceRange,toAddWeightRange,toAddRemainingRange,top=10):
    #make a copy of beers data
    beers = beersData.copy()
    #pre-filter beers status
    #beers = beers.loc[beers["remaining"] >= minRemaining]
    beers = beers.loc[beers["status"] == "available"]

    #get user info
    userId = shippedBoxUser.get("userId")
    freedom = shippedBoxUser.get("freedom")
    userType = shippedBoxUser.get("type")
    #get user strength preferences
    strengthLight = shippedBoxUser.get("strengthLight")
    strengthMedium = shippedBoxUser.get("strengthMedium")
    strengthStrong = shippedBoxUser.get("strengthStrong")
    #get user taste preferences
    tasteStout = shippedBoxUser.get("tasteStout")
    tasteIPA = shippedBoxUser.get("tasteIPA")
    tastePorter = shippedBoxUser.get("tastePorter")
    tasteSour = shippedBoxUser.get("tasteSour")
    #get user region preferences
    regionWestCoast = shippedBoxUser.get("regionWestCoast")
    regionEastCoast = shippedBoxUser.get("regionEastCoast")
    regionMidWest = shippedBoxUser.get("regionMidWest")
    regionEurope = shippedBoxUser.get("regionEurope")

    #get all beer Ids & inventory Ids for those beers we have decided to keep or add
    toKeepBeerIds = []
    toKeepInventoryIds = []
    for oldBoxInventoryId in oldBoxInventoryIds:
        if oldBoxInventoryId not in toRemoveInventoryIds:
            oldBoxBeerId = db1.inventory.find_one({"_id":oldBoxInventoryId}).get("beerId")
            toKeepBeerIds.append(oldBoxBeerId)
            toKeepInventoryIds.append(oldBoxInventoryId)
    for toAddInventoryId in toAddInventoryIds:
        toAddBeerId = db1.inventory.find_one({"_id":toAddInventoryId}).get("beerId")
        toKeepBeerIds.append(toAddBeerId)
        toKeepInventoryIds.append(toAddInventoryId)

    #get number of beers to be edited
    n = len(toRemoveInventoryIds)-len(toAddInventoryIds)
    #start getting recommended beers
    unique = False
    i = 0
    while unique == False:
        #filter by price
        beers = beers.loc[beers.currentPrice >= toAddPriceRange[0]]
        beers = beers.loc[beers.currentPrice <= toAddPriceRange[1]]
        #filter by weight
        beers = beers.loc[beers.weightInMl >= toAddWeightRange[0]]
        beers = beers.loc[beers.weightInMl <= toAddWeightRange[1]]
        #filter by rareness
        beers = beers.loc[beers.remaining >= toAddRemainingRange[0]]
        beers = beers.loc[beers.remaining <= toAddRemainingRange[1]]
        #check
        if beers.shape[0] < n:
            raise ValueError("cannot fulfill the request, please try with different options")
        #get top k beers as candidates
        k = min(top,beers.shape[0])
        recommendedBeers = beers.sort_index(by=['totalSales'],ascending=[False])
        recommendedBeers = recommendedBeers.iloc[random.choice(range(k),n,False)]
        recommendedBeerIds = recommendedBeers["beerId"].tolist()
        newBoxBeerIds = toKeepBeerIds + recommendedBeerIds
        #see whether satisfy the conditions to proceed
        if len(set(newBoxBeerIds)) == len(newBoxBeerIds):
            unique = True
        i += 1
        if i > 10:
            raise ValueError("hitting max attempt for getting unique beers, please try with different options")
    #get inventoryIds
    newBoxInventoryIds = toKeepInventoryIds + getinventoryid(recommendedBeerIds,boxOptions)
    newBoxInfo = {"newBoxInventoryIds":newBoxInventoryIds,"newBoxBeerIds":newBoxBeerIds}
    return(newBoxInfo)

#define the function to get inventory id
def getinventoryid(beerIds,boxOptions):
    inventoryIdList = []
    priceOption = boxOptions.get("price")
    for beerId in beerIds:
        if priceOption != 1:
        #for some beers having multiple styles, choose the one based on the price
            inventoryId = list(db1.inventory.aggregate([ {"$match":{"beerId":beerId}}, {"$sort":{"price.currentPrice":1}},{"$limit":1},{"$project":{"_id":1}}]))
        else:
            inventoryId = list(db1.inventory.aggregate([ {"$match":{"beerId":beerId}}, {"$sort":{"price.currentPrice":-1}},{"$limit":1},{"$project":{"_id":1}}]))
        inventoryIdList.append(str(inventoryId[0].get("_id")))
    return(inventoryIdList)

#insert into mongodb
def insertdb(shippedBoxUser,newBoxInfo):
    #insert shipped beers box to the beerBox collection
    print "inserting recommended beers to the beerBox collection"
    userId = shippedBoxUser.get("userId")
    shippedBoxId = shippedBoxUser.get("shippedBoxId")
    userType = shippedBoxUser.get("type")
    beers = newBoxInfo.get("newBoxBeerIds")
    inventoryIds = newBoxInfo.get("newBoxInventoryIds")
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
    db1.shippedBox.update({"_id":shippedBoxId},{'$set':{"beerBoxId":beerBoxId,"beers":beers,"inventoryIds":inventoryIds}})
    print "done"

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    # client1 = MongoClient('localhost', 27017)
    # db1 = client1.appDB

    #This chunk of code is used to define inputs for testing or debugging
    #See the instructions at line 5
    #number of beers in one beer box, the default one is 6
    #nBeers = 6
    #the propose box id
    shippedBoxId = 'n97ryKBhSBCrHAaZr'
    #a list of beers (inventory ids) to be kept for example:
    toKeepInventoryIds = []
    #or ['1538wrdga4278wbgpkpa16ozbtl','213rrvdga559vbppkg2a12ozbtl']
    #a list of beers (inventory ids) to be added for example:
    toAddInventoryIds = []
    #or ['163clwhba518wxyybbga22ozbtl','234nrhcsa417kcv2013a500mLbtl']
    #a list of beers (inventory ids) to be removed for example:
    toRemoveInventoryIds = []
    #or ['1538wrdga4278wbgpkpa16ozbtl','213rrvdga559vbppkg2a12ozbtl']
    #editing options, a dictionary in the following format:
    boxOptions = {'price':-1,'weight':-1,'rareness':-1}

    #so far we have 6 beers in one beer box
    nBeers = 6
    #check all inputs
    checkallinputs(shippedBoxId,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,nBeers)
    #get all inventory info
    print "getting inventory info with sales data"
    inventoryInfo = getinventory()
    #get location reference
    locationReference = getlocationreference()
    #get sales data
    salesData = cleansalesdata(getsalesdata())
    #add sales data to inventory data
    beersData = addsalesdata(inventoryInfo,salesData)
    #calculate the approximate weight
    beersData = getweight(beersData)
    print str(beersData.shape[0]) + " beers have been loaded"
    #get shipped box info
    shippedBoxUser = getshippedboxuser(shippedBoxId)
    print "the shipped beer box user info:"
    print shippedBoxUser
    #add liked beers to the list 'toKeepInventoryIds'
    toKeepInventoryIds = addtokeep(shippedBoxUser,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,nBeers)
    #get the action
    editAction = geteditaction(toAddInventoryIds,toRemoveInventoryIds)
    print "the action is '" + editAction + "'"
    #execute the action
    newBoxInfo = executeAction(shippedBoxUser,toKeepInventoryIds,toAddInventoryIds,toRemoveInventoryIds,boxOptions,editAction,beersData,nBeers,priceUnit=10,priceVar=5,weightUnit=100,weightVar=100,remainingUnit=0,remainingVar=20)
    #update the shipped beer box
    insertdb(shippedBoxUser,newBoxInfo)
