#this script creates a raw email list containing all unique emails
#this script must be executed only once

#important:
#1: the table "rawEmailList" works as a reference, please do not modify it again once created
#2: define the "email" column as the primary key, easy to update


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd
import csv


#define the function to clean the initial email list
def cleanemaillist(initialEmailList):
    #make a copy of raw data
    emailList = initialEmailList.copy()
    #change data type of userId from string to int
    emailList["userId"] = emailList["userId"].astype(int)
    #change data type of missing values to None
    emailList.loc[emailList.shippingCountry == '#N/A',"shippingCountry"] = None
    emailList.loc[emailList.shippingProvince == '#N/A',"shippingProvince"] = None
    emailList.loc[emailList.shippingCity == '#N/A',"shippingCity"] = None
    return(emailList)

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')
session.row_factory = dict_factory

#load the initial email list locally
print "loading and cleaning the initial email list"
data = []
with open('initial_email_list.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        data.append(row)

#load into a dataframe
initialEmailList = pd.DataFrame(data[1:])
#assign column names
initialEmailList.columns = data[0]
#clean data
initialEmailList = cleanemaillist(initialEmailList)

#create the table for raw unique emails, which won't be touched again
session.execute("""
CREATE TABLE IF NOT EXISTS "rawEmailList" (
    "userId" int,
    email varchar,
    name varchar,
    "shippingCountry" varchar,
    "shippingProvince" varchar,
    "shippingCity" varchar,
    type varchar,
    PRIMARY KEY (email)
)
""")

#insert raw data to cassandra table "rawEmailList"
print "inserting raw initial emails to cassandra, please wait about 2 minutes"
n = initialEmailList.shape[0]
for i in range(n):
    values = initialEmailList.iloc[i].values.tolist()
    prepared_stmt = session.prepare("""
    INSERT INTO "rawEmailList" ("userId",email,name,"shippingCountry","shippingProvince","shippingCity",type)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    bound_stmt = prepared_stmt.bind(values)
    stmt = session.execute(bound_stmt)
print str(n) + " rows of initial emails have been inserted"
