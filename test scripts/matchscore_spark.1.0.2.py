#This script helps to get the match score
#Step 1: get position parameters and user info (user connections info) from mongodb
#Step 2: for each position, get match score for every connection of every bountyme user, filter by a minimum score
#Step 3: get top k connections based on the match score, as recommemdations of each bountyme user for each position
#Step 4: insert everything back to mongodb

#important: for this version, the list of companies and the list of top schools are imported locally
#later on, we will query from mongodb instead

#Load packages
#Load packages
import re
import time
import csv
import pymongo
import pandas
from pandas import DataFrame
from pymongo import MongoClient
from numpy import random
from pyspark import SparkContext

#define the function for cleaning the company list for matching
def cleancompanylist(x):
    #replace "The", "," by ""
    x = re.sub(',','',re.sub('^The ','',x))
    #clean "L.P." & "Inc"
    x = re.sub('L.P.$','',re.sub(' [i|I]nc[/.]?$','',x))
    #clean "Corporation" & "Company"
    x = re.sub(' Corporation$','',re.sub(' Company$','',x))
    #clean "Corp." & "Co."
    x = re.sub(' Corp.$','',re.sub(' Co.$','',x))
    #get lower case
    x = x.lower()
    return(x)

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
def getspeaciallocation(speacialLocation,special):
    def getspeaciallocation_map(x):
        #get category list for special location
        uniqueCategories = speacialLocation.get("importance")
        moreCategories = list(set(speacialLocation.keys()) - set(uniqueCategories))
        rawLocation = x.get("location","").lower().strip()
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
        x[special] = speacialLocation_output
        return(x)
    return(getspeaciallocation_map)

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

#define the function to get the years of experience for each contact
def getexperience(data):
    #first get the current time
    currentDate = time.strftime("%d/%m/%Y")
    currentDay = int(currentDate.split('/')[0])
    currentMonth = int(currentDate.split('/')[1])
    currentYear = int(currentDate.split('/')[2])
    #define a dictionary for converting months to numbers
    #all calculations are based on the month unit
    month_conv = {}
    month_list = ['january','february','march','april','may','june','july','august','september','october','november','december']
    for i, month in enumerate(month_list,1):
        month_conv[month] = i

    #get the years of experience
    for i in range(len(data)):
        #get and clean headline
        headline = data[i].get("headline","")
        if headline == None:
            headline = ''
        else:
            headline = headline.lower()
        #see whether student / professor or not
        if ('student' in headline or 'intern' in headline or
            'candidate' in headline or 'professor' in headline or
            'teacher' in headline or 'dean' in headline):
            totalJobTime = 0
        else:
            #get current and past jobs
            currentJobs = data[i].get("currentJobs",[])
            pastJobs = data[i].get("pastJobs",[])
            #see whether have any currrent job
            #print currentJobs
            if currentJobs == []:
                currentJobTime = 0
            else:
                #get the start date of the last current job
                currentJobDate = currentJobs[-1].get("startDate",'').lower()
                #print currentJobDate
                #if no date, then estimate
                if currentJobDate == '':
                    currentJobTime = 12 * len(currentJobs)
                #if no month is specified, month = 1
                elif len(currentJobDate.split()) == 2:
                    currentJobYear = int(currentJobDate.split()[1])
                    currentJobMonth = month_conv.get(currentJobDate.split()[0])
                else:
                    currentJobYear = int(currentJobDate.split()[0])
                    currentJobMonth = 1
                currentJobTime = (currentYear-currentJobYear)*12 + (currentMonth-currentJobMonth)
            #see whether have any past job
            if pastJobs == []:
                pastJobTime = 0
                limitJobTime = currentJobTime
            else:
                #set initial value
                pastJobTime = 0
                #calculate through each past job
                for pastJob in pastJobs:
                    #get the start date and the end date
                    pastJobStartDate = pastJob.get("startDate","").lower()
                    pastJobEndDate = pastJob.get("endDate","").lower()
                    #print pastJobStartDate + " " + pastJobEndDate
                    if pastJobStartDate == '' or pastJobEndDate == '':
                        continue
                    if len(pastJobStartDate.split()) == 2:
                        pastJobStartYear = int(pastJobStartDate.split()[1])
                        pastJobStartMonth = month_conv.get(pastJobStartDate.split()[0])
                    else:
                        pastJobStartYear = int(pastJobStartDate.split()[0])
                        pastJobStartMonth = 1
                    if len(pastJobEndDate.split()) == 2:
                        pastJobEndYear = int(pastJobEndDate.split()[1])
                        pastJobEndMonth = month_conv.get(pastJobEndDate.split()[0])
                    else:
                        pastJobEndYear = int(pastJobEndDate.split()[0])
                        pastJobEndMonth = 1
                    pastJobTime = pastJobTime + (pastJobEndYear-pastJobStartYear)*12 + (pastJobEndMonth-pastJobStartMonth)
                    #get the possible job time limit by last past job to now
                    if pastJob == pastJobs[-1]:
                        #print "yes"
                        limitJobTime = (currentYear-pastJobStartYear)*12 + (currentMonth-pastJobStartMonth)
            #print str(pastJobTime) + " " + str(limitJobTime)
            #get total job time
            totalJobTime = currentJobTime + pastJobTime
            #check the time limit, making sure total job time <= limit job time
            totalJobTime = min(totalJobTime,limitJobTime)

        #else calculate through each job
        #write to the list
        if totalJobTime < 12:
            yearsOfExperience = "less than 1 year"
        elif totalJobTime >= 12 and totalJobTime < 36:
            yearsOfExperience = "1 to 2 years"
        elif totalJobTime >= 36 and totalJobTime < 60:
            yearsOfExperience = "3 to 5 years"
        elif totalJobTime >= 60 and totalJobTime < 120:
            yearsOfExperience = "6 to 10 years"
        else:
            yearsOfExperience = "more than 10 years"
        data[i]["years of experience"] = yearsOfExperience
        #print yearsOfExperience
        #print data[i].get("years of experience")
    #print data
    return(data)

#define the function to get the seniority level for each contact
def getseniority(x):
    #get and clean headline
    headline = x.get("headline",'')
    if headline == None:
        headline = ''
    else:
        headline = headline.lower()
    #get year of experience
    yearsOfExperience = x.get("years of experience").lower()
    #fisrt filter by title
    #filter by owner/C-level
    if (("president" in headline or "owner" in headline or
        "founder" in headline or "partner" in headline or
        "investor" in headline or "board of directors" in headline or
        "principal" in headline or "chair" in headline or
        "chief" in headline or
        re.search(' c.{1,2}o |^c.{1,2}o ',headline) != None ) and
        ("assistant" not in headline and "vice president" not in headline)):
        seniority = "owner/C-level"
    #filter by student or professor
    elif ('student' in headline or 'intern' in headline or
          'candidate' in headline or
          'professor' in headline or 'instructor' in headline or
          'teacher' in headline or 'dean' in headline or
          'university' in headline or 'college' in headline):
         seniority = "student/professor"
    #filter by manager-level
    elif (("manager" in headline or "director" in headline or
           " vp " in headline or "vice president" in headline or
           "lead" in headline or "head" in headline ) and
          ("assistant" not in headline and
          yearsOfExperience not in ["less than 1 year",])):
        seniority = "manager-level"
    #filter by senior-level
    elif (yearsOfExperience not in ["less than 1 year","1 to 2 years"]):
        seniority = "senior-level"
    #filter by entry-level
    else:
        seniority = "entry-level"
    #print headline + " " + yearsOfExperience
    #print seniority
    x["seniority"] = seniority
    return(x)

#define the function for cleaning the linkedin company name for matching
def cleancompany(x):
    #get cleaned for each contact for both current jobs and past jobs
    companys_list = []
    #first get current jobs
    allJobs = x.get("currentJobs",[]) + x.get("pastJobs",[])
    if allJobs != []:
        for job in allJobs:
            company = job.get("company",'').lower()
            if ("university" in company or "college" in company or
                company == "" or "school" in company):
                companys_list.append("None")
            else:
                companys_list.append(re.sub(',','',re.sub('^the ','',company.lower())))
            #companys_list.append(company)
    x["companies"] = companys_list
    #print allJobs
    #print companys_list
    return(x)

#create an attribute called expertise level
def getlevel(x):
    #initial level is zero for everybody
    x["expertise level"] = 0
    return(x)

#get the general score for education
def geteducationscore(collegeList):
    def geteducationscore_map(x):
        #define the list of master degrees
        masterList = ["master","mba","m.ed","m.s","m.a","ms","jd"]
        #define the list of doctor degrees
        doctorList = ["doctor","phd","ph.d","md"]
        #get current expertise level
        level = x.get("expertise level")
        #get school & degree info
        for education in x.get("education",[]):
            school = education.get("school",'')
            degree = education.get("degree",'')
            #check for None
            if school == None:
                school = ''
            else:
                school = school.lower()
            if degree == None:
                degree = ''
            else:
                degree = degree.lower()
            #a = level
            #get school score
            for targetSchool in collegeList:
                if targetSchool.lower() in school:
                    level = level + 1
                    break
            #print school + " " + str(level - a)
            #get degree score for masters
            for targetDegree in masterList:
                if targetDegree.lower() in degree:
                    level = level + 1
                    break
            #get degree score for doctors
            for targetDegree in doctorList:
                if targetDegree.lower() in degree:
                    level = level + 2
                    break
        x["expertise level"] = level
        return(x)
    return(geteducationscore_map)

#get the general score for seniority & working experience
def getjobscore(x):
    #get current expertise level
    level = x.get("expertise level")
    #get seniority and years of experience
    seniority = x.get("seniority")
    yearsOfExperience = x.get("years of experience")
    #get score for seniority
    seniority_list=["student/professor","entry-level","senior-level","manager-level","owner/C-level"]
    seniorityLevel = seniority_list.index(seniority)
    level = level + seniorityLevel
    #get score for years of experience
    experience_list=["less than 1 year","1 to 2 years","3 to 5 years","6 to 10 years","more than 10 years"]
    experienceLevel = experience_list.index(yearsOfExperience) + 1
    level = level + experienceLevel
    #update expertise level
    x["expertise level"] = level
    return(x)

#get the general score for the number of connections
def getconnectionscore(x):
    #get current expertise level
    level = x.get("expertise level")
    #get the number of connections
    connections = x.get("numConnections",0)
    #get score for connections
    if connections == 500:
        level = level + 2
    elif connections > 300:
        level = level + 1
    x["expertise level"] = level
    return(x)

#get the general score for company
def getcompanyscore(companyList,limit = 10):
    def getcompanyscore_map(x):
        #get current expertise level
        level = x.get("expertise level")
        #get all companies
        companies = x.get("companies",[])
        #get score
        increment = 0
        for company in companies:
            if company in companyList:
                increment = increment + 1
        increment = min(increment,limit)
        #print str(companies) + " " + str(increment)
        x["expertise level"] = level + increment
        return(x)
    return(getcompanyscore_map)

#get everyone sorted and filtered by score
def sortbyscore(data, topK = 20, minScore = 15):
    #sort by level
    sortedByLevelList = sorted(data, key = lambda t: t["expertise level"], reverse = True)
    #filter by top k
    if len(data) < topK:
        data = sortedByLevelList
    else:
        data = sortedByLevelList[0:topK]
    #filter by minimum level
    #data = [contact for contact in sortedByLevelList if contact.get("expertise level") >= minScore]
    return(data)

#define the function for getting position parameters
def getpositions(position,colleges,companies):
    #get position id for this position
    positionId = position.get('_id','')

    #get company name
    name = position.get('jobOverview',{}).get('companyName','')

    #get position title
    title = position.get('jobOverview',{}).get('title','')

    #get general requirements (headline) for this position
    tags_dict = {}
    positionTags = position.get('jobOverview',{}).get('tags',[])
    if  positionTags == None:
        positionTags = []
    for tag in positionTags:
        if tag != None:
            tags_dict[tag.lower()] = 1

    #get education requirements for this position
    #colleges_dict = {"harvard university":1, "carnegie mellon university":1, "california institute of technology":1, "harvey mudd college" :1, "massachusetts institute of technology":1}
    colleges_dict = {}
    for college in colleges:
        if college != None:
            colleges_dict[college.lower()] = 1

    #get requirements for working experience
    #companies_dict = {"amazon":1, "microsoft":1, "space exploration technologies":1}
    companies_dict = {}
    for company in companies:
        if company != None:
            companies_dict[company.lower()] = 1

    #get industry requirements for this position
    industries_dict = {}

    #get skills required for this position
    skills_dict = {}
    positionTechnicalSkills = position.get('jobOverview',{}).get('preferredTechnicalSkills',[])
    positionQualitativeSkills = position.get('jobOverview',{}).get('preferredQualitativeSkills',[])
    if positionTechnicalSkills == None:
        positionTechnicalSkills = []
    if positionQualitativeSkills == None:
        positionQualitativeSkills = []
    for skill in positionTechnicalSkills + positionQualitativeSkills:
        if skill != None:
            skills_dict[skill.lower()] = 1
    #get preferred field for this position
    fields_dict = {}
    positionFields = position.get('jobOverview',{}).get('preferredFields',[])
    if positionFields == None:
        positionFields = []
    for field in positionFields:
        if field != None:
            fields_dict[field.lower()] = 1

    #combine everything as a dictionary
    positionParameters = {}
    positionParameters['id'] = positionId
    positionParameters['name'] = name
    positionParameters['title'] = title
    positionParameters["tags"] = tags_dict
    positionParameters["colleges"] = colleges_dict
    positionParameters["companies"] = companies_dict
    positionParameters["industries"] = industries_dict
    positionParameters["skills"] = skills_dict
    positionParameters["fields"] = fields_dict

    return(positionParameters)

#define the match_score function to get the score for each connection per user with a filter based on the score
def getmatchscore(position, limit = 5):
    def getmatchscore_map(contact):
        #get bountyme id for this user
        bountymeId = contact.get('_id','')

        #get scores for every single connection of this user
        positionTags = position.get('tags',{}).keys()
        positionSkills = position.get('skills',{}).keys()
        positionCompanies = position.get('companies',{}).keys()
        positionColleges = position.get('colleges',{}).keys()
        positionFields = position.get('fields',{}).keys()
        positionDegrees = position.get('degree',{}).keys()

        #set initial value
        score = 0

        #calculate score based on headline
        contactHeadline = contact.get('headline','')
        #print contactHeadline
        for tag in positionTags:
            if tag in contactHeadline.lower():
                score = score + position.get('tags',{}).get(tag,0)
        #print score

        #calculate score based on skills
        contactSkills = ",".join(contact.get('skills',[]))
        scoredSkills = []
        #print contactSkills
        for skill in positionSkills:
            if skill in contactSkills.lower():
                score = score + position.get('skills',{}).get(skill,0)
                scoredSkills.append(skill)
        #print score

        #calculate score based on companies
        #get increment
        increment = 0
        #print contact.get('currentJobs',[]) + contact.get('pastJobs',[])
        for contactJob in contact.get('currentJobs',[]) + contact.get('pastJobs',[]):
            contactCompany = contactJob.get("company",'')
            contactTitle = contactJob.get("title",'')
            #check for None
            if contactCompany == None:
                contactCompany = ''
            else:
                contactCompany = contactCompany.lower()
            if contactTitle == None:
                contactTitle = ''
            else:
                contactTitle = contactTitle.lower()

            for company in positionCompanies:
                #print company
                if company in contactCompany:
                    #print "yes"
                    increment = increment + position.get('colleges',{}).get(company,0)
                    break
            for tag in positionTags:
                if tag in contactTitle:
                    increment = increment + position.get('tags',{}).get(tag,0)
                    break
        score = score + min(increment,2*limit)
        #print score
        #contactEducations = contact.get('education',[])
        #define the list of master degrees
        #masterList = ["master","mba","m.ed","m.s","m.a","ms","jd"]
        #define the list of doctor degrees
        #doctorList = ["doctor","phd","ph.d","md"]

        #calculate score based on education
        #get increment
        increment = 0
        #get school & degree info
        #print contact.get("education",[])
        for contactEducation in contact.get("education",[]):
            contactSchool = contactEducation.get("school",'')
            contactDegree = contactEducation.get("degree",'')
            contactField = contactEducation.get("major",'')
            #check for None
            if contactSchool == None:
                contactSchool = ''
            else:
                contactSchool = contactSchool.lower()
            if contactDegree == None:
                contactDegree = ''
            else:
                contactDegree = contactDegree.lower()
            if contactField == None:
                contactField = ''
            else:
                contactField = contactField.lower()
            #get school score
            for school in positionColleges:
                if school.lower() in contactSchool:
                    increment = increment + position.get('colleges',{}).get(school,0)
                    break
            #get degree score
            for degree in positionDegrees:
                if degree.lower() in degree:
                    increment = increment + position.get('degrees',{}).get(degree,0)
                    break
            #get field score
            for field in positionFields:
                if field.lower() in contactField:
                    increment = increment + position.get('fields',{}).get(field,0)
                    break
        score = score + min(increment,limit)
        contactCopy = contact.copy()
        contactCopy["match score"] = score
        contactCopy["scoredSkills"] = scoredSkills
        #print score
        return(contactCopy)
    return(getmatchscore_map)

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
    sortedByKeyList = sorted(contacts, key = lambda t: t["match score"], reverse = True)
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
def getcontacts(userId,degree = "first"):
    #db.LinkedInCollectionTest.find({"identity.bountyUserId":{"$in":userIds}}):
    #for contact in client.linkedinDB.Profiles.find({"connectionIds":{}):
    #    contacts.append(contact)
    #sprint client.linkedinDB.Profiles.find_one()
    contacts = []
    #for contact in client.linkedinDB.Profiles.find({"connectionIds":{"$in": userId}}):
    #    contacts.append(contact)
    for contact in client.linkedinDB.Profiles.find({"education":{"$elemMatch":{"school":'University of California, Berkeley',"major":"Computer Science"}}}):
        contacts.append(contact)
    print len(contacts)
    return(contacts)

#conf spark
sc = SparkContext("local","spark test")

#retrieve data for all positions
client = MongoClient('localhost', 3001)
db = client.meteor
positions = []
positionIds = []
for job in db.positions.find():
    positions.append(job)
    positionIds.append(job.get('_id',''))

#######################
#the following session will be deleted
#######################
#import the list of broad locations firstly
data = []
with open('BroadLocation.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#convert raw data to a dictionary locally
broadLocation = getlocationcategory(data)
#print broadLocation

#then repeat for functional location
data = []
with open('FunctionalLocation.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#convert raw data to a dictionary
functionalLocation = getlocationcategory(data)

#import company list & school list locally
with open('TopSchoolList.csv', 'rb') as inputCollegeList:
    reader = csv.reader(inputCollegeList)
    collegeList = list(reader)[0]

with open('Fortune500.csv', 'rb') as inputCompanyList:
    reader = csv.reader(inputCompanyList)
    companyList = list(reader)[0]

#clean company list first for matching
companyList = sc.parallelize(companyList).map(cleancompanylist).collect()

#retrieve data for all bountyme users
client = MongoClient('192.168.18.49')
client.linkedinDB.authenticate('userApp', 'raja123', mechanism='MONGODB-CR')
uri = "mongodb://userApp:raja123@192.168.18.49/linkedinDB?authMechanism=MONGODB-CR"
client = MongoClient(uri)
db = client.meteor

contacts = []
for contact in client.linkedinDB.Profiles.find():
    contacts.append(contact)

#get special location tags for each contacts
contacts = sc.parallelize(contacts).map(getspeaciallocation(broadLocation,special="broad location")).map(getspeaciallocation(broadLocation,special="functional location")).collect()

#get experience level
contacts = getexperience(contacts)
contacts = sc.parallelize(contacts).map(getseniority).map(getlevel).map(getconnectionscore).map(getjobscore).map(cleancompany)

#update level for education & company
contacts = contacts.map(geteducationscore(collegeList)).map(getcompanyscore(companyList))

#get all position requirements
positionParameters = []
for everyPosition in positions:
    positionParameters.append(getpositions(everyPosition,collegeList,companyList))

#print positionParameters

#get scores
#we will go through every position, and search for every bountyme user,
#and get top k recommendations per user
#linkedinUsers.append({})
#contacts = contacts[100:103]
#positionParameters = positionParameters[0:2]
#print positionParameters
output = []
for positionParameter in positionParameters:
    output_dict = {}
    output_dict["positionId"] = positionParameter.get('id','')
    output_dict["positionTitle"] = positionParameter.get('title','')
    #recommendations = []
    #for contact in contacts:
    #    recommendations.append(getmatchscore(contact,positionParameter))
    recommendations = contacts.map(getmatchscore(positionParameter)).collect()
    #    recommendations.append(getconnections(sortbyuser(getscores(everyUser,positionParameter))))
    #output_dict["recommendations"] = sortbyscore(recommendations)
    output_dict["recommendations"] = sortbyscore(recommendations)
    output.append(output_dict)
    #print recommendations

#temp = getcontacts(['yjmqu8M2pLmja6bXF',"sdasdas"])
#print temp
#Principal Software Engineer, Ads Platform Technology, Zynga

# for item in output:
#     positionRecommendation = {}
#     positionRecommendation["positionId"] = item.get('positionId','')
#     matches = []
#     for connection in item.get('recommendations',[]):
#         matches = matches + connection.get('connections',[])
#     #print matches
#     positionRecommendation["matches"] = removedup(sortbyposition(matches))
#
#     print positionRecommendation
#
#     db.PositionRecommendations.find_one_and_update({"positionId":positionRecommendation.get('positionId')}, {'$set': {'matches': positionRecommendation.get('matches')}}, upsert=True)


print output

#db.PositionRecommendations.insert_one(output)
