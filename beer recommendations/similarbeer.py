#This script generates similar and recommended beers using sales data grouped by geographic
#locations
#only similar beers will be inserted into MongoDB, while recommended beers
#won't be inserted into MongoDB for now
#input collections: "rawSalesData" and "beers"
#outputs collections: "beers"


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
from bson.objectid import ObjectId


#define the function to get sales data from mongodb
def importsalesdata():
    sales = []
    for transaction in db1.rawSalesData.find():
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
    return(x)

#define the function to get the best-selling beers
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

#define the dfunction for dimension reduction (PCA) so that it can save time when runing k-means
def reducedimension(x, v = 0.99, k = 20):
    #get first k components or first n components having variance explained > v
    pca = PCA()
    pca.fit(x)
    variance = np.cumsum(pca.explained_variance_ratio_)
    ncomp = min(np.where(variance > v)[0][1],k)
    return(pca.transform(x)[:,0:ncomp])

#define the function to fit k-means for different number of iterations for tuning
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
    return(x)

#define the function to get beer group labels
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
    #when new beers are available, they won't have a group label because they have no sales
    #so the similar beers of "new beers" will be best-selling beers for now
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

#define the function for getting recommended beer for different groups of customers
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

#define the function to add more beers into the recommended beer list as user defined (not finished)
def getmorebeer():
    moreBeers = {}
    bestBeers = getbestbeer(beer, k = 5)
    rareBeers = []
    discountedBeers = []
    moreBeers["bestBeers"] = bestBeers
    moreBeers["rareBeers"] = rareBeers
    moreBeers["discountedBeers"] = discountedBeers
    return(moreBeers)

#define the function to get beer id for similar/recommended beers
def getbeerid(x, beerIds_dict, group):
    for item in x:
        beers = []
        for beer in item.get(group):
            beers.append(beerIds_dict.get(beer,''))
        item[group] = beers
    return(x)

#insert into mongodb (similar beers only)
def insertdb(beerOutput):
    print "inserting similar beers into MongoDB"
    for i,item in enumerate(beerOutput):
        if item.get("beerId") != '':
            db1.beers.update({"_id":item.get("beerId")},{'$set':{'similarBeers':item.get("similarBeer")}},upsert=True)
    print str(i+1) + " sets of similar beers have been inserted"


#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    # client1 = MongoClient('localhost', 27017)
    # db1 = client1.appDB

    #get all existing beer ids from beers collection
    beerIds_dict = {}
    for beer in db1.beers.find():
        beerName = beer.get('overview',{}).get('name','')
        beerIds_dict[beerName] = beer.get("_id","")
    #get sales data
    print "loading sales data from mongodb"
    beer = importsalesdata()
    #get some more beers for recommended, as user defined (it is empty so far)
    moreBeers = getmorebeer()

    #find similar beers & similar customers based on geographic location
    #the first version is using "state" as address for non-CA customers; use "city" for CA customers
    #see the other versions as the commented code in the bottom
    print "cleaning the raw data"
    beer = getlocation(beer,states = ["California"])
    #get cleaned data and set up clusters for beers
    beerLocSales = getdata(beer, value = "totalSales",column = "location")
    #use PCA to reduce dimensions to speed up the computation
    smallBeer = reducedimension(beerLocSales,v = 0.99,k = 300)
    #find out the best number of k for k-means clusters
    #findk(smallBeer, n = 50, maxK = 100)
    #according to the results printed by findK(), we choose 50 groups, K = 50
    #set n as number of iterations
    #large n will give you better clusters with longer time
    print "working on the generating similar and recommended beers, please wait about 2 minutes"
    beer0 = getlabels(beerLocSales, smallBeer, n = 100, k = 50)
    #set up clusters for customers
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

    #insert similar beers only
    insertdb(beerOutput)




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
