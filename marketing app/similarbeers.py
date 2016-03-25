#This script helps to find similar/recommended beers by geographic location (sales)
#The input is all transactions from 2014 Jan to 2015 June
#The printouts are # of beers in each beer group, # of types of customers in each customer group
#The outputs are two lists of dictionaries of similar & recomended beers


#Load packages
import csv
import pandas as pd
from pandas import DataFrame
import numpy as np
import Pycluster
import sys
import pymongo
from pymongo import MongoClient
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from bson.objectid import ObjectId
#reload(sys)
#sys.setdefaultencoding("utf-8")


#define the function to get missing beers without sales data
def getmissingbeer(x, y):
    missingBeers = list(set(y.keys()) - set(x["productTitle"]))
    #print len(missingBeers)
    return(missingBeers)

#define the function to get sales data from mongo
def importsalesdata():
    sales = []
    for transaction in db1.rawSalesData.find():
        #print sales
        sales.append(transaction)
    sales = DataFrame(sales)
    #create a column for unique city
    sales["shippingAddress"] = sales["shippingCity"] + " " + sales["shippingProvince"]
    return(sales)

#define the function to reshape data
def getdata(x, row = "productTitle",value = "totalSales", column = "shippingProvince"):

    #change data types
    x[["totalSales","price","quantityCount"]] = x[["totalSales","price","quantityCount"]].astype(float)
    x[["quantityCount"]] = x[["quantityCount"]].astype(int)
    #clean data
    x = x.query('shippingProvince != "" and productTitle != "" and totalSales >= 0')
    x = x.query('productType != "Gift Card" and productType != "Subscription" and productType != ""')
    #subset data only for product title, shipping provingce and the Y variable we want
    x = x[[row,column,value]]
    #reshape data by location
    x = pd.pivot_table(x, index = [row], columns = [column],
                            values = [value], aggfunc = np.sum, fill_value = 0)
    return(x)

#define the function to get locations for k-means
def getlocation(x,states):
    #get locations
    x["location"] = x["shippingProvince"]
    #get locations for states in the list which we want to break out into cities
    for state in states:
        x.loc[x["shippingProvince"] == state,"location"] = x.loc[x["shippingProvince"] == state,"shippingAddress"]
    #x.loc[x["shipping_province"] in states,"location"] = x.loc[x["shipping_province"] in states,"shipping_address"]
    return(x)

#define the function to get the best beers
def getbestbeer(x,k = 5):
    #change data types
    x[["totalSales","price","quantityCount"]] = x[["totalSales","price","quantityCount"]].astype(float)
    x[["quantityCount"]] = x[["quantityCount"]].astype(int)
    #clean data
    x = x.query('shippingProvince != "" and productTitle != "" and totalSales >= 0')
    x = x.query('productType != "Gift Card" and productType != "Subscription" and productType != ""')
    #get best beers
    newx = pd.pivot_table(x,index=['productTitle'],values=['totalSales'],aggfunc=np.sum,fill_value=0)
    new = newx.sort('totalSales',ascending = False).index.values.tolist()[0:k]
    return(new)

#define the dfunction for dimension reduction so that it can save time when runing k-means
def reducedimension(x, v = 0.99, k = 20):
    #get first k components or first n components having variance explained > v
    pca = PCA()
    pca.fit(x)
    variance = np.cumsum(pca.explained_variance_ratio_)
    ncomp = min(np.where(variance > v)[0][1],k)
    return(pca.transform(x)[:,0:ncomp])

#define the function to fit k-means a number of times to find the best number of clusters
def findk(x, n = 1000, minK = 2, maxK = 20):
    errors = []
    #fit k-means clusters for n times
    for i in range(minK,maxK+1,1):
        _, error, nfound = Pycluster.kcluster(x, nclusters = i, transpose=0,
                                         method='a', dist='e', npass = n)
        #get errors
        errors.append(error)
        print i
    print errors

#define the function to get the labels of k-means
def getlabels(x, y, n = 1000 , k = 8):
    if y == "none":
        y = x
    #fit k-means clusters
    labels, _, _ = Pycluster.kcluster(y, nclusters = k, transpose=0,
                                     method='a', dist='e', npass = n)
    #write labels back
    x.loc[:,"group"] = labels
    #count how many items in each group
    labels = list(labels)
    for i in range(k):
        print labels.count(i)
    return(x)

#############################################################

#No longer need the sections shown below:

#############################################################

#define the function to find robust k-means centers
def findcenters(x,n=1000,k=6):
    #get dimensions
    m = x.shape[1]
    #create centers as empty
    centers = DataFrame(np.zeros(shape=(k,m)))

    for i in range(n):
        labels, _, _ = Pycluster.kcluster(x, nclusters = k, transpose=0,
                                        method='a', dist='e', npass = 1)
        center, _ = Pycluster.clustercentroids(x,clusterid = labels)
        #sort centers by the distance to the origin
        center = sorted(center,key = lambda t: np.linalg.norm(np.array(t)-np.zeros(m)), reverse = True)

        #print np.linalg.norm(np.array(center[0])-np.zeros(m))
        #print np.linalg.norm(np.array(center[1])-np.zeros(m))
        #print np.linalg.norm(np.array(center[2])-np.zeros(m))
        #print np.linalg.norm(np.array(center[3])-np.zeros(m))
        #print np.linalg.norm(np.array(center[4])-np.zeros(m))
        #print np.linalg.norm(np.array(center[5])-np.zeros(m))
        #print np.array(center[0])
        #print np.array(center[1])
        #print np.array(center[2])
        #print np.array(center[3])
        #print np.array(center[4])
        #print np.array(center[5])
        #take the average
        for j in range(k):
            centers.ix[j,:] = centers.ix[j,:] + center[j]
    centers = centers/n
    return(centers)

#define the function to find the labels for each beer
def findlabels(x,centers):
    #get dimensions
    m = x.shape[0]
    n = centers.shape[0]
    #get labels by finding the cloest center for each point
    labels = []
    for i in range(m):
        dist = []
        for j in range(n):
            dist.append(np.linalg.norm(np.array(x.ix[i,:])-np.array(centers.ix[j,:])))
        label = [s for s, t in enumerate(dist) if t == min(dist)][0]
        labels.append(label)
    #write labels back
    x.loc[:,"group"] = pd.Series(labels, index = x.index)
    #count how many items in each group
    for i in range(n):
        print labels.count(i)
    return(x)

#############################################################

#define the function to write beer labels back to the raw data
def attachlabels(x,y):
    #get dimension
    n = y.shape[0]
    #convert labels to a list
    group = y["group"].tolist()
    #write procuct_title => group in a dictionary format
    labels_dict = {}
    for i in range(n):
        labels_dict[y.index.values[i]] = group[i]
    #write labels to the raw data
    labels_list = []
    products = x["productTitle"].tolist()
    for product in products:
        labels_list.append(labels_dict.get(product,"exclude"))
    x.loc[:,"beerGroup"] = pd.Series(labels_list, index = x.index)
    #clean data
    x = x.query('beerGroup != "exclude"')
    x = x.query('shippingProvince != "" and productTitle != "" and totalSales >= 0')
    x = x.query('productType != "Gift Card" and productType != "Subscription" and productType != ""')
    return(x)

#define the function to write customer groups back to the raw data
def attachgroups(x,y,column = "shippingProvince"):
    #get dimension
    n = y.shape[0]
    #convert groups to a list
    group = y["group"].tolist()
    #write shipping provinces => group in a dictionary format
    groups_dict = {}
    for i in range(n):
        groups_dict[y.index.values[i]] = group[i]
    groups_list = []
    provinces = x[column].tolist()
    for province in provinces:
        groups_list.append(groups_dict.get(province,"exclude"))
    x.loc[:,"customerGroup"] = pd.Series(groups_list, index = x.index)
    #clean data
    x = x.query('customerGroup != "exclude"')
    x = x.query('shippingProvince != "" and productTitle != "" and totalSales >= 0')
    x = x.query('productType != "Gift Card" and productType != "Subscription" and productType != ""')
    return(x)

#define the function for getting similar beer
def getsimilarbeer(x, beerIds_dict, k = 20, l = 40):
    #add 1 to k in order to remove the beer itself from the similar beer list later on
    k = k + 1
    #get number of beer groups
    n = max(x.beerGroup) + 1
    #create the output list
    beer_output = []
    #get top beers for each group
    top_beer = []
    for i in range(n):
        #split data by two groups
        group1 = x.loc[x.beerGroup == i,['productTitle','totalSales']]
        group2 = x.loc[x.beerGroup != i,['productTitle','totalSales']]
        group1 = pd.pivot_table(group1, index = ['productTitle'],
                                values = ['totalSales'], aggfunc = np.sum, fill_value = 0)
        group2 = pd.pivot_table(group2, index = ['productTitle'],
                                values = ['totalSales'], aggfunc = np.sum, fill_value = 0)
        #if # of similar beers is larger than # of beers in that group, append top beers from other groups
        if group1.shape[0] >= k:
            beer_list = group1.sort('totalSales',ascending = False)[0:k]
            beer_similar = beer_list.index.values.tolist()
        else:
            beer_list1 = group1.sort('totalSales',ascending = False)
            beer_list2 = group2.sort('totalSales',ascending = False)[0:k-group1.shape[0]]
            beer_similar1 = beer_list1.index.values.tolist()
            beer_similar2 = beer_list2.index.values.tolist()
            beer_similar = beer_similar1 + beer_similar2
        top_beer.append(beer_similar)
    #get predicted similar beers for new beers
    new = {}
    new["beerId"] = ''
    new["productTitle"] = "new beer"
    new["beerGroup"] = "none"
    newx = pd.pivot_table(x,index=['productTitle'],values=['totalSales'],aggfunc=np.sum,fill_value=0)
    new["similarBeer"] = newx.sort('totalSales',ascending = False).index.values.tolist()[0:l]
    beer_output.append(new)
    #reshape data
    x = pd.pivot_table(x, index = ['productTitle'],
                            values = ['beerGroup'], aggfunc = np.min, fill_value = 0)
    #get similar beers for each product
    for i in range(x.shape[0]):
        beer_item = {}
        beer_item["productTitle"] = x.index.values.tolist()[i]
        beer_item["beerId"] = beerIds_dict.get(beer_item.get("productTitle",''),'')
        label = x["beerGroup"][i]
        beer_item["beerGroup"] = label
        #check whether the beer itself is in the similar beer list
        if beer_item.get("productTitle") in top_beer[label]:
            new_top_beer = [item for item in top_beer[label] if item != beer_item.get("productTitle")]
            beer_item["similarBeer"] = new_top_beer
        else:
            beer_item["similarBeer"] = top_beer[label][0:k-1]
        beer_output.append(beer_item)
    return(beer_output)

#define the function for getting recommended beer
def getrecommendedbeer(x, y, k=20, address = "shippingProvince"):
    #get number of customer groups
    n = max(x.customerGroup) + 1
    #create the output list
    customer_output = []
    #get top beers for each group
    top_beer = []
    for i in range(n):
        #split data by two groups
        group1 = x.loc[x.customerGroup == i,['productTitle','totalSales']]
        group2 = x.loc[x.customerGroup != i,['productTitle','totalSales']]
        group1 = pd.pivot_table(group1, index = ['productTitle'],
                                values = ['totalSales'], aggfunc = np.sum, fill_value = 0)
        group2 = pd.pivot_table(group2, index = ['productTitle'],
                                values = ['totalSales'], aggfunc = np.sum, fill_value = 0)
        #if # of recommended beers is larger than # of beers in that group, append top beers from other groups
        if group1.shape[0] >= k:
            beer_list = group1.sort('totalSales',ascending = False)[0:k]
            beer_recommended = beer_list.index.values.tolist()
        else:
            beer_list1 = group1.sort('totalSales',ascending = False)
            beer_list2 = group2.sort('totalSales',ascending = False)[0:k-group1.shape[0]]
            beer_recommended1 = beer_list1.index.values.tolist()
            beer_recommended2 = beer_list2.index.values.tolist()
            beer_recommended = beer_recommended1 + beer_recommended2
        top_beer.append(beer_recommended)
    #get predicted recommended beers for new customers
    more = y.get("bestBeers",[]) + y.get("rareBeers",[]) + y.get("discountedBeers",[])
    new = {}
    new["address"] = "new customer"
    new["customerGroup"] = "none"
    newx = pd.pivot_table(x,index=['productTitle'],values=['totalSales'],aggfunc=np.sum,fill_value=0)
    rec = newx.sort('totalSales',ascending = False).index.values.tolist()[0:k]
    add = []
    for item in more:
        if item not in rec:
            add.append(item)
    new["recommendedBeer"] = rec[0:k-len(add)] + add
    customer_output.append(new)
    #reshape data
    x = pd.pivot_table(x, index = [address],
                            values = ['customerGroup'], aggfunc = np.min, fill_value = 0)
    #get recommended beers for each customer
    for i in range(x.shape[0]):
        customer = {}
        customer[address] = x.index.values.tolist()[i]
        label = x["customerGroup"][i]
        customer["customerGroup"] = label
        #add top beers, rare beers and discounted beers
        rec = top_beer[label]
        add = []
        for item in more:
            if item not in rec:
                add.append(item)
        #note that the number of more beers <= k
        customer["recommendedBeer"] = rec[0:k-len(add)] + add
        customer_output.append(customer)
    return(customer_output)

#define the function to setting a random selection for each beer/customerOutput
def getrandom(x, group, k = 20, n = 3):
    #for each item, from k candidates (similar beer/recommended beer), we randomly get n of them
    for item in x[1:]:
        np.random.shuffle(item.get(group))
        item[group] = item.get(group)[0:n]
    return(x)

#define the function to get beer id for similar/recommended beers
def getbeerid(x, beerIds_dict, group):
    #for item in enumerate(x):
    for item in x:
        beers = []
        for beer in item.get(group):
            # beers_dict = {}
            # beers_dict["name"] = beer
            # beers_dict["beerId"] = beerIds_dict.get(beer,'')
            beers.append(beerIds_dict.get(beer,''))
        item[group] = beers
    return(x)

#connect to the Mongodb
print "connecting to the mongodb from the local server"
client = MongoClient('localhost', 3001)
db1 = client.meteor
# client1 = MongoClient('localhost', 27017)
# db1 = client1.appDB

#get beer ids
beerIds_dict = {}
for beer in db1.beers.find():
    beerName = beer.get('overview',{}).get('name','')
    beerIds_dict[beerName] = beer.get("_id","")

#get sales data
print "loading sales data from mongodb"
beer = importsalesdata()

#get some more beers for recommended, as user defined
moreBeers = {}
bestBeers = getbestbeer(beer, k = 5)
rareBeers = []
discountedBeers = []
moreBeers["bestBeers"] = bestBeers
moreBeers["rareBeers"] = rareBeers
moreBeers["discountedBeers"] = discountedBeers


#find similar beers & similar customers based on geographic location

#the first version is using "state" as address for non-CA customers; use "city" for CA customers
print "cleaning the raw data"
beer = getlocation(beer,states = ["California"])
#get cleaned data basd on total sales
beerLocSales = getdata(beer, value = "totalSales",column = "location")
#use PCA to reduce dimensions to speed up the computation
smallBeer = reducedimension(beerLocSales,v = 0.99,k = 300)
#find out the best number of k for k-means clusters
#findk(smallBeer, n = 50, maxK = 100)
#we use k = 50, and the following progress will take a long time
print "working on the generating similar beers, please wait about 2 minutes"
beer0 = getlabels(beerLocSales, smallBeer, n = 200, k = 50)
#beer0 = getlabels(beerLocSales, n = 100, k = 50)
#get customer data
newbeer = attachlabels(x = beer, y = beer0)
customerLocSales = getdata(newbeer, row = "location", value = "totalSales", column = "beerGroup")
#find out the best number of k for k-means clusters
#findk(customerLocSales, n = 1000, maxK = 20)
#we use k = 8
customer0 = getlabels(customerLocSales, y = "none", n = 1000, k = 8)
#get output for beers
beerOutput = getsimilarbeer(x = newbeer, beerIds_dict = beerIds_dict, k = 20)
#get output for customers
newcustomer = attachgroups(x = beer, y = customer0, column = "location")
customerOutput = getrecommendedbeer(x = newcustomer, y = moreBeers, k = 20, address = "location")
#randomize the output
beerOutput = getrandom(beerOutput, group = "similarBeer")
customerOutput = getrandom(customerOutput, group = "recommendedBeer")
#get beer ids for all similar beers and recommended beers
beerOutput = getbeerid(beerOutput, beerIds_dict, group = "similarBeer")
customerOutput = getbeerid(customerOutput, beerIds_dict, group = "recommendedBeer")

#there are 4 more versions for featuring testing

#version 1: state & total sales
#get cleaned data based on total sales
#beerSales = getdata(beer, value = "total_sales")
#find out the best number of k for k-means clusters
#findk(beerSales, n = 100, maxK = 30)
#we use k = 50
#beer1 = getlabels(beerSales, n = 100, k = 50)
#get customer data
#newbeer = attachlabels(x = beer, y = beer1)
#customerSales = getdata(newbeer, raw = "shipping_province", value = "total_sales", column = "beerGroup")
#find out the best number of k for k-means clusters
#findk(customerSales, n = 1000, maxK = 20)
#we use k = 8
#customer1 = getlabels(customerSales, n = 1000, k = 8)
#get output for beers
#beerOutput = getsimilarbeer(x = newbeer)
#get output for customers
#newcustomer = attachgroups(x = beer, y = customer1)
#customerOutput = getrecommendedbeer(x = newcustomer, y = moreBeers)

#version 2: state & quantity count
#get cleaned data basd on quantity counts
#beerCount = getdata(beer, value = "quantity_count")
#find out the best number of k for k-means clusters
#findk(beerCount, n = 100, maxK = 30)
#we use k = 8
#beer2 = getlabels(beerCount,n = 100, k = 8)
#get customer data
#newbeer = attachlabels(x = beer, y = beer2)
#customerCount = getdata(newbeer, raw = "shipping_province", value = "quantity_count", column = "beerGroup")
#find out the best number of k for k-means clusters
#findk(customerCount, n = 1000, maxK = 20)
#we use k = 8
#customer2 = getlabels(customerCount, n = 1000, k = 8)
#get output for beers
#beerOutput = getsimilarbeer(x = newbeer)
#get output for customers
#newcustomer = attachgroups(x = beer, y = customer2)
#customerOutput = getrecommendedbeer(x = newcustomer)

#version 3: city & total sales
#get cleaned data basd on total sales
#beerCitySales = getdata(beer, value = "total_sales",column = "shipping_address")
#find out the best number of k for k-means clusters
#findk(beerCitySales, n = 50, maxK = 20)
#we use k = 8
#beer3 = getlabels(beerCitySales, n = 100, k = 8)
#get customer data
#newbeer = attachlabels(x = beer, y = beer3)
#customerCitySales = getdata(newbeer, raw = "shipping_address", value = "total_sales", column = "beerGroup")
#find out the best number of k for k-means clusters
#findk(customerCitySales, n = 100, maxK = 20)
#we use k = 8
#customer3 = getlabels(customerCitySales, n = 1000, k = 8)
#get output for beers
#beerOutput = getsimilarbeer(x = newbeer)
#get output for customers
#newcustomer = attachgroups(x = beer, y = customer3,column = "shipping_address")
#customerOutput = getrecommendedbeer(x = newcustomer, address = "shipping_address")

#version 4: city & quantity count
#get cleaned data basd on quantity count
#beerCityCount = getdata(beer, value = "quantity_count",column = "shipping_address")
#find out the best number of k for k-means clusters
#findk(beerCityCount, n = 50, maxK = 20)
#we use k = 8
#beer4 = getlabels(beerCityCount, n = 100, k = 8)
#get customer data
#newbeer = attachlabels(x = beer, y = beer4)
#customerCityCount = getdata(newbeer, raw = "shipping_address", value = "quantity_count", column = "beerGroup")
#find out the best number of k for k-means clusters
#findk(customerCityCount, n = 100, maxK = 20)
#we use k = 8
#customer4 = getlabels(customerCityCount, n = 1000, k = 8)
#get output for beers
#beerOutput = getsimilarbeer(x = newbeer)
#get output for customers
#newcustomer = attachgroups(x = beer, y = customer4,column = "shipping_address")
#customerOutput = getrecommendedbeer(x = newcustomer, address = "shipping_address")

#print beerOutput
#print customerOutput

print "updating data to mongodb"
for i,item in enumerate(beerOutput):
    #print item
    if item.get("beerId") != '':
        db1.beers.update({"_id":item.get("beerId")},{'$set':{'similarBeers':item.get("similarBeer")}},upsert=True)
        print str(i) + " sets of similar beers have been inserted"
