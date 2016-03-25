#this script checks all KPIs of our marketing process


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd
import numpy as np


#define the function to track the email campaign processes
def checkemails(centralEmailList,sentEmailList):
    #check total number of emails and user type
    regularUser = centralEmailList[centralEmailList["type"] == "regular"].shape[0]
    memberUser = centralEmailList[centralEmailList["type"] == "member"].shape[0]
    print str(centralEmailList.shape[0]) + "distinct emails are activated."
    print str(regularUser) + " emails are regular users, and " + str(memberUser) + " emails are club members."
    #check email status
    pendingUser = centralEmailList[centralEmailList["status"] == "pending"].shape[0]
    sentUser = centralEmailList[centralEmailList["status"] == "sent"].shape[0]
    rejectedUser = centralEmailList[centralEmailList["status"] == "rejected"].shape[0]
    print str(pendingUser) + " emails are pending. " + str(sentUser) + " emails are sent. " + str(rejectedUser) + " emails are rejected"
    #check recommended beers type
    regularBeers = centralEmailList[centralEmailList["beersType"] == "regular"].shape[0]
    print str(regularBeers) + "emails are assigned with regular beers."
    similarBeers = centralEmailList[centralEmailList["beersType"] == "similar"].shape[0]
    print str(similarBeers) + "emails are assigned with similar beers."
    popularBeers = centralEmailList[centralEmailList["beersType"] == "popular"].shape[0]
    print str(popularBeers) + "emails are assigned with popular beers."
    #check recommended beers
    recommendedBeers = centralEmailList["recommendedBeers"].values.tolist()
    n = len(recommendedBeers[0])
    for i,beers in enumerate(recommendedBeers):
        if len(beers) != n:
            raise ValueError("The sizes among recommended beers don't match; The " + str(i) + "th user has size " + str(len(beers)) + " not " + str(n))
    print "The size of recommended beers now is " + str(n) + "."
    #check response info
    noResponse = sentEmailList[sentEmailList["response"] == "no"].shape[0]
    openResponse = sentEmailList[sentEmailList["response"] == "open"].shape[0]
    clickResponse = sentEmailList[sentEmailList["response"] == "click"].shape[0]
    soldResponse = sentEmailList[sentEmailList["response"] == "sold"].shape[0]
    print "For all " + str(sentUser) + " sent emails: "+ str(noResponse) + " no responses, " + str(openResponse) + " opened, " + str(clickResponse) + " clicked " + str(soldResponse) + " sold"


#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

centralEmailList = session.execute("""
select * from "centralEmailList"
""")

sentEmailList = session.execute("""
select * from "sentEmailList"
""")

centralEmailList = pd.DataFrame(list(centralEmailList))
sentEmailList = pd.DataFrame(list(sentEmailList))
checkemails(centralEmailList,sentEmailList)
