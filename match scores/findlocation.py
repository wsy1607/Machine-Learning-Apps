#This script helps to get special location infomation
#broad locations are shown below:
#northwest, southwest, midwest, northeast, southeast, west coast, east coast, canada, east asia, south asia
#functional locations are shown below:
#south california, north california, san francisco bay area, new york area, los angeles area, boston area,
#atlanta area, seattle area, denver area, chicago area

#important: for this version, the list of broad locations and the list of functional locations are local
#later on, we will query from mongodb instead

#Load packages
import csv
import pymongo
import pandas
from pandas import DataFrame
from pymongo import MongoClient

#define the function for converting data frame to a list of dictionaries
def getusers(data):
    #get all headers as keys
    keys = data.columns.values
    #create the empty list as the output
    data_list = []
    for i in range(data.shape[0]):
        data_dict = {}
        for key in keys:
            data_dict[key] = data.loc[i,key]
        data_list.append(data_dict)
    return(data_list)

#define the function to get a dictionary for raw data
def getlocationcategory(data):
    #create the empty dictionary as the output
    location_dict = {}
    for i in range(len(data)):
        row = data[i]
        #clean the empty strings attached
        if '' in row:
            #get the first place we have the empty string in that row
            k = row.index('')
        else:
            k = len(row)
        location_dict[row[0]] = row[1:k]
    return(location_dict)

#define the function to get the special locations (broad / functional) for each contacts
def getspeaciallocation(data,speacialLocation,special):
    #get category list for special location
    uniqueCategories = speacialLocation.get("importance")
    moreCategories = list(set(speacialLocation.keys()) - set(uniqueCategories))
    for i in range(len(data)):
        #get and clean the location
        rawLocation = data[i].get("location","").lower().strip()
        location = rawLocation.replace(" area","").replace(",","").replace("st. ","st-")
        #break out each location into parts, and then partition them
        locationPartitions = getlocationpartition(location)
        #print location
        #print locationPartitions
        #create output list
        speacialLocation_output = []
        #create a boolean variable for exiting the outer loop
        exitFlag = False
        #go for unique categories with each partititon
        for category in uniqueCategories:
            if exitFlag == True:
                break
            for locationPartition in locationPartitions:
                if locationPartition in speacialLocation.get(category):
                    speacialLocation_output.append(category)
                    exitFlag = True
                    break
        #go for additional categories with each partition
        for category in moreCategories:
            for locationPartition in locationPartitions:
                if locationPartition in speacialLocation.get(category):
                    speacialLocation_output.append(category)
                    break
        data[i][special] = speacialLocation_output
    return(data)

#define the function to break out each location into parts, and then repartition them
def getlocationpartition(location):
    #break out the location
    locationUnits = location.split(" ")
    #now only consider first k parts, k <= 4
    n = min(len(locationUnits),4)
    #get location partitions
    locationPartitions = []
    for j in range(1,n+1):
        if j == 1:
            locationPartitions = locationPartitions + locationUnits
        elif j == 2:
            for k in range(1,n):
                locationSeq = [locationUnits[k-1], locationUnits[k]]
                locationPartitions.append(" ".join(locationSeq))
        elif j == 3:
            for k in range(1,n-1):
                locationSeq = [locationUnits[k-1], locationUnits[k], locationUnits[k+1]]
                locationPartitions.append(" ".join(locationSeq))
        else:
            locationPartitions.append(" ".join(locationUnits))
        #print locationPartitions
    return(locationPartitions)

#connect to the Mongodb
client = MongoClient('192.168.18.49')
client.linkedinDB.authenticate('userApp', 'raja123', mechanism='MONGODB-CR')
uri = "mongodb://userApp:raja123@192.168.18.49/linkedinDB?authMechanism=MONGODB-CR"
client = MongoClient(uri)
db = client.meteor

#load linkedin data
users = []
for user in client.linkedinDB.Profiles.find():
    users.append(user)

#print users

#######################
#the following session will be deleted
#######################

# #import linkedin contacts locally
# rawdata = []
# with open('LinkedinContacts.csv', 'rb') as csvfile:
#     reader = csv.reader(csvfile)
#     for row in reader:
#         rawdata.append(row)
#
# #load into a dataframe
# data = DataFrame(rawdata[1:])
# #assign column names
# data.columns = rawdata[0]
# #convert data frame to a list of dictionaries
# users = getusers(data)

#import location lists, broad & functional locally
data = []
with open('BroadLocation.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#convert raw data to a dictionary
broadLocation = getlocationcategory(data)
#print broadLocation

#repeat
data = []
with open('FunctionalLocation.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#convert raw data to a dictionary
functionalLocation = getlocationcategory(data)
#print functionalLocation

#get special location tags for each contacts
users = getspeaciallocation(data=users,speacialLocation=broadLocation,special="Broad Location")
users = getspeaciallocation(data=users,speacialLocation=functionalLocation,special="Functional Location")
print users


#######################

#retrieve data for all positions
#positions = []
#positionIds = []
#for job in db.positions.find():
#    positions.append(job)
#    positionIds.append(job.get('_id',''))

#retrieve data for all bountyme users
#users = []
#userIds = []
#linkedinUsers = []
#for user in db.users.find():
#    users.append(user)
#    userIds.append(user.get('_id',''))

#retrieve data for all connections for every bountyme user
#for linkedinUser in db.LinkedInCollectionTest.find({"identity.bountyUserId":{"$in":userIds}}):
#    linkedinUsers.append(linkedinUser)

#get all position requirements
#positionParameters = []
#for everyPosition in positions:
#    positionParameters.append(getpositions(everyPosition,collegeList,companyList))

#get scores
#we will go through every position, and search for every bountyme user,
#and get top k recommendations per user
#linkedinUsers.append({})
#output = []
#for positionParameter in positionParameters:
#    output_dict = {}
#    output_dict["positionId"] = positionParameter.get('id','')
#    output_dict["positionTitle"] = positionParameter.get('title','')
#    recommendations = []
#    for everyUser in linkedinUsers:
#        recommendations.append(getconnections(sortbyuser(getscores(everyUser,positionParameter))))
#    output_dict["recommendations"] = recommendations
#    output.append(output_dict)


#for item in output:
#    positionRecommendation = {}
#    positionRecommendation["positionId"] = item.get('positionId','')
#    matches = []
#    for connection in item.get('recommendations',[]):
#        matches = matches + connection.get('connections',[])
    #print matches
#    positionRecommendation["matches"] = removedup(sortbyposition(matches))

#    print positionRecommendation

#    db.PositionRecommendations.find_one_and_update({"positionId":positionRecommendation.get('positionId')}, {'$set': {'matches': positionRecommendation.get('matches')}}, upsert=True)



#print output

#db.PositionRecommendations.insert_one(output)
