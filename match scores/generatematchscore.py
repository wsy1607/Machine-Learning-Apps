#This script generates match scores between contacts and job posts
#contacts are based on users' linkedin connections
#input collections: "fortune500","topSchool","positions" and "profiles"
#output collections: undefined
#important: should execute this script after new job are available in the database
#note that we don't have any insertion because the data structure needs to be re-defined

#Step 1: get position info including all requirements
#Step 2: get user's connections as candidates
#Step 3: calculate the match score for every candidate
#Step 4: insert everything back to mongodb

#to do: define the data structure and then complete those score sorting functions
#from line 263 to line 306 and finish the insertion


#load packages
import re
import time
import csv
import pymongo
import pandas
from pandas import DataFrame
from pymongo import MongoClient
from numpy import random
from bson.objectid import ObjectId


#define the function to check the inputs
def checkinputs(positionId,userId,connectionDegree):
    #check the degree of connection
    if connectionDegree not in ["first","second","third"]:
        raise ValueError("the degree of connection must be first, second or third")
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
        print "calculating match score for the position: " + positionTitle + " using " + userName + "'s " + connectionDegree + " degree connections"

#define the function for querying connections of users, usually the first degree
def getcontacts(userId,connectionDegree = "first"):
    contacts = []
    firstConnectionIds = []
    secondConnectionIds = []
    #get all first degree connections
    for firstConnection in db1.profiles.find({"connectionIds":{"$elemMatch":{"$eq":userId}}}):
        firstConnectionIds += firstConnection.get("connectionIds",[])
        contacts.append(firstConnection)
    #get all second degree connections if necessary
    if connectionDegree != "first":
        for secondConnection in db1.profiles.find({"connectionIds":{"$elemMatch":{"$in":firstConnectionIds}}}):
            if secondConnection not in contacts:
                secondConnectionIds += secondConnection.get("connectionIds",[])
                contacts.append(secondConnection)
    #get all third degree connections if necessary
    elif connectionDegree != "second":
        for thirdConnection in db1.profiles.find({"connectionIds":{"$elemMatch":{"$in":secondConnectionIds}}}):
            if thirdConnection not in contacts:
                contacts.append(thirdConnection)
    return(contacts)

#define the function to get the top school list
def getcollegelist():
    collegeList = db1.topSchool.find_one().get("topSchool")
    return(collegeList)

#define the function to get the top company list
def getcompanylist():
    companyList = db1.fortune500.find_one().get("fortune500")
    return(companyList)

#define the function for cleaning the company list for matching
def cleancompanylist(companyList):
    #each of the company name in each profile list should be trimmed to improve the matching accuracy
    for i, company in enumerate(companyList):
        #clean "The" & ","
        companyCleaned1 = re.sub(',','',re.sub('^The ','',company))
        #clean "L.P.", "Ltd." and "Inc"
        companyCleaned2 = re.sub(' LLC[/.]$','',re.sub(' Ltd[/.]$', '',re.sub(' L.P.$','',re.sub(' [i|I]n[c|s][/.]?$','',companyCleaned1))))
        #clean "Corporation", "Company" and "Incorporated"
        companyCleaned3 = re.sub(' Incorporated$','',re.sub(' Corporation$','',re.sub(' Company$','',companyCleaned2)))
        #clean "Corp.", "Cos." and "Co."
        companyCleaned4 = re.sub(' Cos[/.]$','',re.sub(' Corp[/.]$','',re.sub(' Co[/.]$','',companyCleaned3)))
        #clean "Holdings", "Group"
        companyCleaned5 = re.sub(' Group','',re.sub(' Holdings','',companyCleaned4))
        companyList[i] = companyCleaned5.lower()
    return(companyList)

#define the function for cleaning the company list for matching
def cleancompany(company):
    #clean "The" & ","
    companyCleaned1 = re.sub(',','',re.sub('^The ','',company))
    #clean "L.P.", "Ltd." and "Inc"
    companyCleaned2 = re.sub(' LLC[/.]$','',re.sub(' Ltd[/.]$', '',re.sub(' L.P.$','',re.sub(' [i|I]n[c|s][/.]?$','',companyCleaned1))))
    #clean "Corporation", "Company" and "Incorporated"
    companyCleaned3 = re.sub(' Incorporated$','',re.sub(' Corporation$','',re.sub(' Company$','',companyCleaned2)))
    #clean "Corp.", "Cos." and "Co."
    companyCleaned4 = re.sub(' Cos[/.]$','',re.sub(' Corp[/.]$','',re.sub(' Co[/.]$','',companyCleaned3)))
    #clean "Holdings", "Group"
    companyCleaned5 = re.sub(' Group','',re.sub(' Holdings','',companyCleaned4))
    #convert to the lower case
    return(companyCleaned5.lower())

#define the function for getting position parameters defined in the job post
def getpositions(position,colleges,companies):
    #get position id for this position
    positionId = position.get('_id','')
    #get company info
    name = position.get('jobOverview',{}).get('companyName','')
    #get position title
    title = position.get('jobOverview',{}).get('title','')
    #get general requirements (for headline) for this position
    tagsDict = {}
    positionTags = position.get('jobOverview',{}).get('tags',[])
    if  positionTags == None:
        positionTags = []
    for tag in positionTags:
        if tag != None:
            tagsDict[tag.lower()] = 1
    #get education requirements for this position
    collegesDict = {}
    for college in colleges:
        if college != None:
            collegesDict[college.lower()] = 1
    #get requirements for working experience
    companiesDict = {}
    for company in companies:
        if company != None:
            companiesDict[company.lower()] = 1
    #get industry requirements for this position which will be updated later
    industriesDict = {}
    #get skills required for this position
    skillsDict = {}
    positionTechnicalSkills = position.get('jobOverview',{}).get('preferredTechnicalSkills',[])
    positionQualitativeSkills = position.get('jobOverview',{}).get('preferredQualitativeSkills',[])
    if positionTechnicalSkills == None:
        positionTechnicalSkills = []
    if positionQualitativeSkills == None:
        positionQualitativeSkills = []
    for skill in positionTechnicalSkills + positionQualitativeSkills:
        if skill != None:
            skillsDict[skill.lower()] = 1
    #get preferred field for this position
    fieldsDict = {}
    positionFields = position.get('jobOverview',{}).get('preferredFields',[])
    if positionFields == None:
        positionFields = []
    for field in positionFields:
        if field != None:
            fieldsDict[field.lower()] = 1
    #combine everything as a dictionary
    positionParameters = {}
    positionParameters['id'] = positionId
    positionParameters['name'] = name
    positionParameters['title'] = title
    positionParameters["tags"] = tagsDict
    positionParameters["colleges"] = collegesDict
    positionParameters["companies"] = companiesDict
    positionParameters["industries"] = industriesDict
    positionParameters["skills"] = skillsDict
    positionParameters["fields"] = fieldsDict
    return(positionParameters)

#define the match score function to get the score for each contact
def getmatchscore(contact, position, jobLimit = 5, schoolLimit = 5):
    #get score for every single connection of this user
    positionTags = position.get('tags',{}).keys()
    positionSkills = position.get('skills',{}).keys()
    positionCompanies = position.get('companies',{}).keys()
    positionColleges = position.get('colleges',{}).keys()
    positionFields = position.get('fields',{}).keys()
    positionDegrees = position.get('degree',{}).keys()
    #set initial value
    score = 0
    #calculate score based on headline (tags)
    contactHeadline = contact.get('headline','')
    for tag in positionTags:
        if tag in contactHeadline.lower():
            score += position.get('tags',{}).get(tag,0)
    #calculate score based on skills
    contactSkills = ",".join(contact.get('skills',[]))
    scoredSkills = []
    for skill in positionSkills:
        if skill in contactSkills.lower():
            score += position.get('skills',{}).get(skill,0)
            scoredSkills.append(skill)
    #calculate score based on companies which must be within the limit
    increment = 0
    for contactJob in contact.get('currentJobs',[]) + contact.get('pastJobs',[]):
        contactCompany = contactJob.get("company",'')
        contactTitle = contactJob.get("title",'')
        #clean the company name
        if contactCompany == None:
            contactCompany = ''
        else:
            contactCompany = cleancompany(contactCompany)
        #clean the position title
        if contactTitle == None:
            contactTitle = ''
        else:
            contactTitle = contactTitle.lower()
        for company in positionCompanies:
            if company in contactCompany:
                increment += position.get('companies',{}).get(company,0)
                break
        for tag in positionTags:
            if tag in contactTitle:
                increment += position.get('tags',{}).get(tag,0)
                break
    #the score increment in the job section can not be greater than the limit
    score += min(increment,jobLimit)
    #calculate score based on education
    #get increment
    increment = 0
    #get school & degree info
    for contactEducation in contact.get("education",[]):
        contactSchool = contactEducation.get("school",'')
        contactDegree = contactEducation.get("degree",'')
        contactField = contactEducation.get("major",'')
        #clean the school name
        if contactSchool == None:
            contactSchool = ''
        else:
            contactSchool = contactSchool.lower()
        #clean the degree name
        if contactDegree == None:
            contactDegree = ''
        else:
            contactDegree = contactDegree.lower()
        #clean the field name
        if contactField == None:
            contactField = ''
        else:
            contactField = contactField.lower()
        #get school score
        for school in positionColleges:
            if school.lower() in contactSchool:
                increment += position.get('colleges',{}).get(school,0)
                break
        #get degree score
        for degree in positionDegrees:
            if degree.lower() in degree:
                increment += position.get('degrees',{}).get(degree,0)
                break
        #get field score
        for field in positionFields:
            if field.lower() in contactField:
                increment += position.get('fields',{}).get(field,0)
                break
    score += min(increment,schoolLimit)
    contactCopy = contact.copy()
    contactCopy["matchScore"] = score
    contactCopy["scoredSkills"] = scoredSkills
    contactCopy["numScoredSkills"] = len(scoredSkills)
    return(contactCopy)

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

#main function
if __name__ == '__main__':
    #connect to mongodb
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor

    #get inputs for debugging
    positionId = '6iPyWnpCc9Wism3Eh'
    userId = 'yjmqu8M2pLmja6bXF'
    connectionDegree = "second"
    checkinputs(positionId,userId,connectionDegree)

    #retrieve data for this position
    print "getting the job position and linkedin contacts info from the local server"
    position = db1.positions.find_one({'_id':positionId})
    print "the job description of position id: " + positionId + " has been loaded"
    contacts = getcontacts(userId,connectionDegree)
    print str(len(contacts)) + " contacts are loaded"
    #get the college and company lists
    print "loading the college and companies lists from mongodb"
    collegeList = getcollegelist()
    #clean company list first for matching
    companyList = cleancompanylist(getcompanylist())
    #get all position requirements
    print "working on calculating the match score"
    positionParameters = getpositions(position,collegeList,companyList)
    #get match scores
    output = []
    for contact in contacts:
        output.append(getmatchscore(contact,positionParameters))
    print output[1:10]
    print "done"
    #insert to MongoDB ...
