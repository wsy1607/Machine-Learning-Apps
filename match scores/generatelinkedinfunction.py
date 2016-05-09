#This script gets some functional infomation for all new contacts
#input collections: "fortune500","topSchool","broadLocation","functionalLocation" and "profiles" (remote server)
#output collections: "profiles" (local server)
#important: should execute this script very frequently after new contacts are available in the database

#function details are shown as below:
#function 1: years of experience (professional experience):
#less than 1 year, 1 to 2 years, 3 to 5 years, 6 to 10 years, more than 10 years
#function 2: seniority:
#student / professor, entry-level, senior-level, manager-level, owner/C-level
#function 3: broad location:
#northwest, southwest, midwest, northeast, southeast,west coast, east coast, canada, east asia, south asia
#function 4: functional location (US only):
#south california, north california, san francisco bay area, greater new york area, greater los angeles area,
#greater boston area, greater atlanta area, greater seattle area, greater denver area, greater chicago area
#function 5: expertise level:
#a general score calculated based on the given profile measured by the professional experience
#education, working and networking experience would be considered
#function 6: current job time:
#a metric showing how many months this person has been working at the current position

#to do: add more functions to clean company names, position titles, school names and degrees


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

#define the function to get the broad location list
def getbroadlocation():
    broadLocation = {}
    for location in db1.broadLocation.find():
        broadLocation[location.get("locationName")] = location.get("locationList")
    return(broadLocation)

#define the function to get the functional location list
def getfunctionallocation():
    functionalLocation = {}
    for location in db1.functionalLocation.find():
        functionalLocation[location.get("locationName")] = location.get("locationList")
    return(functionalLocation)

#define the function to get the top school list
def getcollegelist():
    collegeList = db1.topSchool.find_one().get("topSchool")
    return(collegeList)

#define the function to get the top company list
def getcompanylist():
    companyList = db1.fortune500.find_one().get("fortune500")
    return(companyList)

#define the function to get the special locations (broad / functional) for each contacts
def getspeaciallocation(data,speacialLocation,special):
    #get category list for special location
    uniqueCategories = speacialLocation.get("importance")
    moreCategories = list(set(speacialLocation.keys()) - set(uniqueCategories))
    for i in range(len(data)):
        #get and clean the location
        rawLocation = data[i].get("location","").lower().strip()
        location = rawLocation.replace(" area","").replace(",","").replace("st. ","st-")
        #break down each location into parts, and then partition them
        locationPartitions = getlocationpartition(location)
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

#define the function to break down each location into parts, and then repartition them
def getlocationpartition(location):
    #break down the location
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

    #get the years of experience and the current job time
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
            currentJobTime = 0
        else:
            #get current and past jobs
            currentJobs = data[i].get("currentJobs",[])
            pastJobs = data[i].get("pastJobs",[])
            #see whether have any currrent job
            if currentJobs == []:
                currentJobTime = 0
            else:
                #get the start date of the last current job
                currentJobDate = currentJobs[-1].get("startDate",'').lower()
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
                        limitJobTime = (currentYear-pastJobStartYear)*12 + (currentMonth-pastJobStartMonth)
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
        data[i]["yearsOfExperience"] = yearsOfExperience
        data[i]["currentJobTime"] = str(currentJobTime) + " months"
    return(data)

#define the function to get the promotion indicator
def getpromotionindicator(data, month = 12):
    for i in range(len(data)):
        if data[i].get("currentJobTime",0) > month:
            promotionIndicator = 1
        else:
            promotionIndicator = 0
        data[i]["promotionIndicator"] = promotionIndicator
    return(data)

#define the function to get the seniority level for each contact
def getseniority(data):
    for i in range(len(data)):
        #get and clean headline
        headline = data[i].get("headline",'')
        if headline == None:
            headline = ''
        else:
            headline = headline.lower()
        #get year of experience
        yearsOfExperience = data[i].get("yearsOfExperience").lower()
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
        #filter by recruiter
        elif ('recruit' in headline or 'talent acquisition' in headline or
              'human resources' in headline or ' hr ' in headline):
            seniority = "recruiter"
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
        data[i]["seniority"] = seniority
    return(data)

#define the function to clean linkedin company names and write to a new list
def cleancompany(data):
    #get cleaned for each contact for both current jobs and past jobs
    for i, contact in enumerate(data):
        companys_list = []
        #first get current jobs
        allJobs = contact.get("currentJobs",[]) + contact.get("pastJobs",[])
        if allJobs != []:
            for job in allJobs:
                company = job.get("company",'').lower()
                if ("university" in company or "college" in company or
                    company == "" or "school" in company):
                    companys_list.append("None")
                else:
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
                    companys_list.append(companyCleaned5.lower())
        data[i]["companies"] = companys_list
    return(data)

#define the function to get the latest degree for each contact
def getlatestdegree(data):
    for i, contact in enumerate(data):
        degrees = []
        #get education info
        for education in contact.get("education",[]):
            degree = education.get("degree")
            if degree != None:
                degrees.append(degree)
        #get the latest degree
        if degrees == []:
            latestDegree = None
        else:
            latestDegree = degrees[0]
        data[i]["latestDegree"] = latestDegree
    return(data)

#create an attribute called expertise level
def getlevel(data):
    #initial level is zero for everybody
    for i, contact in enumerate(data):
        data[i]["expertiseLevel"] = 0
    return(data)

#get the general score for education
def geteducationscore(data,collegeList):
    #define the list of master degrees
    masterList = ["master","mba","m.ed","m.s","m.a","ms","jd"]
    #define the list of doctor degrees
    doctorList = ["doctor","phd","ph.d","md"]
    for i, contact in enumerate(data):
        #get current expertise level
        level = contact.get("expertiseLevel")
        #get school & degree info
        for education in contact.get("education",[]):
            school = education.get("school",'')
            degree = education.get("degree",'')
            #check for missing values
            if school == None:
                school = ''
            else:
                school = school.lower()
            if degree == None:
                degree = ''
            else:
                degree = degree.lower()
            #get school score
            for targetSchool in collegeList:
                if targetSchool.lower() in school:
                    level = level + 1
                    break
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
        data[i]["expertiseLevel"] = level
    return(data)

#get the general score for working experience
def getjobscore(data):
    for i, contact in enumerate(data):
        #get current expertise level
        level = contact.get("expertiseLevel")
        #get seniority and years of experience
        seniority = contact.get("seniority")
        yearsOfExperience = contact.get("yearsOfExperience")
        #get score for seniority
        seniority_list=["student/professor","recruiter","entry-level","senior-level","manager-level","owner/C-level"]
        seniorityLevel = seniority_list.index(seniority)
        level = level + seniorityLevel
        #get score for years of experience
        experience_list=["less than 1 year","1 to 2 years","3 to 5 years","6 to 10 years","more than 10 years"]
        experienceLevel = experience_list.index(yearsOfExperience) + 1
        level = level + experienceLevel
        #update expertise level
        data[i]["expertiseLevel"] = level
    return(data)

#get the general score for networking (number of connections)
def getconnectionscore(data):
    for i, contact in enumerate(data):
        #get current expertise level
        level = contact.get("expertiseLevel")
        #get the number of connections
        connections = contact.get("numConnections",0)
        #get score for connections
        if connections == 500:
            level += 2
        elif connections > 300:
            level += 1
        data[i]["expertiseLevel"] = level
    return(data)

#get the general score for company
def getcompanyscore(data,companyList,limit = 5):
    for i, contact in enumerate(data):
        #get current expertise level
        level = contact.get("expertiseLevel")
        #get all companies
        companies = contact.get("companies",[])
        #get score
        increment = 0
        for company in companies:
            if company in companyList:
                increment += 1
        increment = min(increment,limit)
        data[i]["expertiseLevel"] = level + increment
    return(data)

#define the function for querying connections for specific users (debug only)
def getcontacts(userId,companyList,degree = "first"):
    #db.LinkedInCollectionTest.find({"identity.bountyUserId":{"$in":userIds}}):
    #for contact in client.linkedinDB.Profiles.find({"connectionIds":{}):
    #    contacts.append(contact)
    #sprint client.linkedinDB.Profiles.find_one()
    #there are a few ways to filter contacts
    #an example of filter by education
    # contacts = []
    # for contact in client.linkedinDB.Profiles.find({"education":{"$elemMatch":{"school":'University of California, Berkeley',"major":"Computer Science"}}}):
    #     contacts.append(contact)
    # print len(contacts)
    # #an example of filter by current company
    # contacts = []
    # for contact in client.linkedinDB.Profiles.find({"currentJobs":{"$elemMatch":{"company":"Google"}}}):
    #     contacts.append(contact)
    # print len(contacts)
    #an example of filter by first degree connection
    contacts = []
    for contact in client2.linkedinDB.Profiles.find({"connectionIds":{"$in": userId}}):
        contacts.append(contact)
    print len(contacts)
    #an example of filter by second degree connection
    # contacts = []
    # firstConnectionIds = []
    # for firstConnectionId in client.linkedinDB.Profiles.find({"connectionIds":{"$in": userId}}):
    #     firstConnectionIds += firstConnectionId.get("connectionIds",[])
    # for contact in client.linkedinDB.Profiles.find({"connectionIds":{"$in":firstConnectionIds}}):
    #     contacts.append(contact)
    # print len(contacts)
    return(contacts)

#insert profiles data into MongoDB
def insertdb(contacts):
    print "inserting data into mongodb"
    for i,contact in enumerate(contacts):
        #basic info
        contactId = contact.get("_id")
        name = contact.get("name")
        publicLink = contact.get("publicLink")
        emails = contact.get("emails")
        location = contact.get("location")
        headline = contact.get("headline")
        headline2 = contact.get("headline2")
        currentJobs = contact.get("currentJobs")
        pastJobs = contact.get("pastJobs")
        education = contact.get("education")
        skills = contact.get("skills")
        connectionIds = contact.get("connectionIds")
        numConnections = contact.get("numConnections")
        #functional info
        currentJobTime = contact.get("currentJobTime")
        functionalLocation = contact.get("functionalLocation")
        broadLocation = contact.get("broadLocation")
        companies = contact.get("companies")
        yearsOfExperience = contact.get("yearsOfExperience")
        seniority = contact.get("seniority")
        latestDegree = contact.get("latestDegree")
        expertiseLevel = contact.get("expertiseLevel")
        #insert into mongodb
        db1.profiles.insert({
        "_id":str(contactId),
        "name":name,
        "publicLink":publicLink,
        "emails":emails,
        "location":location,
        "headline":headline,
        "headline2":headline2,
        "currentJobs":currentJobs,
        "pastJobs":pastJobs,
        "education":education,
        "skills":skills,
        "connectionIds":connectionIds,
        "numConnections":numConnections,
        "currentJobTime":currentJobTime,
        "functionalLocation":functionalLocation,
        "broadLocation":broadLocation,
        "companies":companies,
        "yearsOfExperience":yearsOfExperience,
        "seniority":seniority,
        "latestDegree":latestDegree,
        "expertiseLevel":expertiseLevel,
        })
    print str(i+1) + " new contacts have been successfully inserted"

#main function
if __name__ == '__main__':
    #connect to mongodb locally as db1
    print "connecting to mongodb from the local server"
    client1 = MongoClient('localhost', 3001)
    db1 = client1.meteor
    #connect to mongodb remotely as db2
    print "connecting to the remote server"
    client2 = MongoClient('192.168.18.49')
    client2.linkedinDB.authenticate('userApp', 'raja123', mechanism='MONGODB-CR')
    uri = "mongodb://userApp:raja123@192.168.18.49/linkedinDB?authMechanism=MONGODB-CR"
    client2 = MongoClient(uri)
    db2 = client2.meteor

    #load the list of locations, schools and companies
    print "loading location, college and companies lists from mongodb"
    broadLocation = getbroadlocation()
    functionalLocation = getfunctionallocation()
    collegeList = getcollegelist()
    #clean company list first for matching
    companyList = cleancompanylist(getcompanylist())

    print "getting linkedin contacts"
    #get all existing contacts
    oldContactIds = []
    for profile in db1.profiles.find():
        oldContactIds.append(profile.get("_id","none"))
    #get all new contacts and exclude old contacts
    contacts = []
    for contact in client2.linkedinDB.Profiles.find():
        #make sure these contacts are new
        if contact.get("_id") not in oldContactIds:
            contacts.append(contact)

    print str(len(contacts)) + " new contacts are loaded"
    print "working on linkedin functions"
    #get special location tags for each contacts
    contacts = getspeaciallocation(data=contacts,speacialLocation=broadLocation,special="broadLocation")
    contacts = getspeaciallocation(data=contacts,speacialLocation=functionalLocation,special="functionalLocation")
    #get years of experience for users
    contacts = getexperience(contacts)
    #get promotion indicator for users
    contacts = getpromotionindicator(contacts)
    #get seniority for users
    contacts = getseniority(contacts)
    #get the latest degree
    contacts = getlatestdegree(contacts)
    #get initial expertise level
    contacts = getlevel(contacts)
    #update level for education
    contacts = geteducationscore(contacts, collegeList = collegeList)
    #update level for job & connections
    contacts = getconnectionscore(getjobscore(contacts))
    #update level for company
    contacts = getcompanyscore(cleancompany(contacts), companyList = companyList)
    #insert to mongodb
    insertdb(contacts)
