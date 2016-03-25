#this script inserts sales data from 2014-Jan to cassandra database as raw sales data


#load packages
from cassandra.cluster import Cluster
import pandas as pd
import time
import csv

#connect to cassandra
cluster = Cluster()
session = cluster.connect('demodb')

# session.execute("""
# insert into company (company_name, company_city, size) values ('bountyme', 'san francisco', 10)
# """)

# prepared_stmt = session.prepare ( "INSERT INTO company (company_name, company_city, size) VALUES (?, ?, ?)")
# bound_stmt = prepared_stmt.bind(['python', 'unknown', 100])
# stmt = session.execute(bound_stmt)
#
# result = session.execute("select * from company")
#load packages
# from cassandra.cluster import Cluster
#
# #connect to cassandra
# cluster = Cluster()
# session = cluster.connect('craftshack')
#
# create keyspace if not exists craftshack with replication = {'class':'SimpleStrategy','replication_factor':3};
#
#
# session.execute("""
# CREATE TABLE IF NOT EXISTS rawSalesData (
#     orderId int,
#     productType varchar,
#     vendorName varchar,
#     productTitle varchar,
#     variantTitle varchar,
#     price float,
#     name varchar,
#     email varchar,
#     shippingCountry varchar,
#     shippingProvince varchar,
#     shippingCity varchar,
#     year varchar,
#     month varchar,
#     day varchar,
#     quantityCount int,
#     totalSales float,
#     orderCount int,
#     PRIMARY KEY (orderId)
# )
# """)

# #load the initial email list locally
# print "loading the initial email list"
# data = []
# with open('initial_email_list.csv', 'rb') as csvfile:
#     reader = csv.reader(csvfile)
#     for row in reader:
#         data.append(row)
#
# initialEmailList = pd.DataFrame(data[1:])
# #assign column names
# initialEmailList.columns = data[0]
# initialEmailList.loc[initialEmailList.shippingCountry == '#N/A',"shippingCountry"] = None
# print initialEmailList
#print initialEmailList.loc[initialEmailList.shippingCountry == '#N/A']

#print (time.strftime("%m/%d/%Y"))
