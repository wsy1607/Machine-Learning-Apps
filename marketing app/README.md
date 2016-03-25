# Marketing App

This is an email marketing process of email campaigns for all 7000+ potential customers. This program will control the process from back-end using python and could be interacted with the front-end interface as well.

files can be found at: https://drive.google.com/open?id=0B5wkYHJz9Ns8azJBQjZQWHNoMDA

github repository: https://github.com/wsy1607/Marketing-App


## To Do

1: get the real version info when creating the version info table

2: fix missing data for user preferences and beer parameters


## Overview of process
First set up all raw data. Then generate emails for every single marketing campaign using multiple versions for testing. Update version ratios for next campaign using multi-armed bandit test, and update ranks of all customers based on the predicted likelihood of getting positively feedback by machine learning algorithms.


## Tools and Database Models
Use the python scripts to read and write data into Cassandra, which stores all backend data. The following 4 steps include all methods. All methods from step 2 to step 4 are found in the emailCampain project under tasks.py (under celery tasks). Methods in step 1 should be executed only once when setting up all the data infrastructure (they won't show up in the celery tasks folder).


### Step 1: Setting up all Raw Data and the central email list
Use keyspace "marketingApp" in Cassandra, and run all python scripts listed below once. See all data schema in the database reference section.

* Cassandra Setup: go to the Cassandra main repository, enter "bin/apache-cassandra -f" to run the Cassandra database

* cassandrasetup.py: creates the main keyspace "marketingApp"

* rawsalesdata.py creates the table "rawSalesData" using raw_sales_data.csv which shows some historical transactions by those potential customers.

* rawemaillist.py creates the table "rawEmailList" using initial_email_list.csv which is a list of all unique customers with their emails and other personal data from shopify sales data.

* rawversioninfo.py creates the table "rawVersionInfo" and "versionTests". "rawVersionInfo" keeps all raw version information, and "versionTests" keeps AB split-tests results.

* mongodbsetup.py creates a list beers with sales, similar beers and ranks, which will be displayed as recommendations.

* centralemaillist.py creates the table "centralEmailList" using the table "rawSalesData" and "rawEmailList". It is the full list of all customers with emails and other personal data. Each email in the "centralEmailList" has a status and a rank. We will select emails from the "centralEmailList" based on the status and the rank for every single campaign. The status and rank will be updated after completing each email campaign.


### Step 2: Sending a single campaign (the first campaign)
After setting up all raw data, we are able to run all python scripts listed below once to send an email campaign.

* createproposeemails.py creates the table "proposeEmailList" using the table "centralEmailList", which has the next campaign candidates. We get these propose emails from the central email list with 'preferred' status or having 'pending' status with highest ranks.

* The administrator can manually check for all those propose emails. When generating the propose email candidates, the default 'check' status is "yes". If the administrator doesn't want to consider this email forever, turn it to "no". If the administrator doesn't want to consider this email for the next campaign, turn it to "pending".

* addproposeemails.py updates the table "proposeEmailList" by adding more propose emails when we don't have enough propose emails available.

* createfinalemails.py creates the table "finalEmailList" using the table "proposeEmailList" with 'yes' check status, and all emails on the "finalEmailList" will be send out for the next campaign. In the main time, we assign each email a version randomly based on the most recent multi-armed-split-test ratio calculated from the table "versionTests".

* The administrator is able to send all emails in the table "finalEmailList".

* cleanfinalemail.py creates or updates the table "sentEmailList" by appending all sent emails in the "finalEmailList" and updates the table "centralEmailList" by new status and drops the table "proposeEmailList" and the table "finalEmailList".


### Step 3: Recalculate version test ratios and email ranks
After collecting some feedbacks from those sent emails, such as 'open', 'reply' or 'interested', we recalculate version test ratios using multi-armed-split-tests and email ranks using machine learning algorithms.

* updateversionratio.py updates version ratios using the table "sentEmailList". We append all ratios so that we can track all ratios for each campaign.

* updateemailrank.py updates all email ranks in the table "centralEmailList" using "sentEmailList". Given those feedback from those sent emails, apply machine learning algorithms to predict the probability that this email user is valuable. Then update the rank based on this probability, which is a major reference for selecting propose emails for the next campaign.

* updateemailstatus.py (optional) updates all email status in the table "centralEmailList" by the email campaign administrator from the front-end. Inputs are a list of emails and a status so that every email would be manually controlled.

### Step 4: Repeat the step 2 and step 3 until we complete the process

* tracking.py reports all KPIs of our marketing applications to track the whole process including general emails information and sent emails information.

## Database Reference

* Keyspace: craftshack

* All Tables: "rawSalesData","rawEmailList","versionInfo","beersData","centralEmailList","proposeEmailList"(temp),"finalEmailList"(temp),"sentEmailList","versionTests"

* Raw Data: "rawSalesData","rawEmailList","versionInfo"

* Campaign Data: "beersData","centralEmailList","proposeEmailList","finalEmailList"

* Analytic Data: "sentEmailList","versionTests"


## Table Reference


### Table Indices

* "rawSalesData": no indices

* "rawEmailList": no indices

* "versionInfo": no indices

* "beersData": no indices

* "centralEmailList": indices on "status", "type", "rank"

* "proposeEmailList": indices on "status", "check", "type"

* "finalEmailList": indices on "status", "check", "type", "versionId"

* "sentEmailList": indices on "response", "age", "gender", "type"

* "versionTests": no indices

### Column References

* "orderId" int: the unique order or transaction id obtained from shopify sales data
* "productType" varchar: the type of the product, such as beer, gift or subscription
* "vendorName" varchar: the name of the vendor
* "productTitle" varchar: the title or name of the product
* "variantTitle" varchar: the title or name of the product size and container info
* price float: the price sold at
* name varchar: name of the customer
* email varchar: the email address of the customer
* "shippingCountry" varchar: country shipped to
* "shippingProvince" varchar: province or state shipped to
* "shippingCity" varchar: city shipped to
* month varchar: month of this order
* day varchar: day of this order
* "quantityCount" int: the number of products ordered
* "totalSales" float: total sales
* "orderCount" int: the number of orders of this transaction
* "updateDate" varchar: the last date updating the rank
* "updateTime" timestamp: the last time updating the rank
* "userId" int: the unique user id for this email campaign process
* type varchar: the type of this user, either regular or member
* "lastOrderDate" varchar: the date of the last order
* orders set<int>: all orders by this user as a list of order ids
* preferences set<varchar>: user preferences for taste, ABV or beer region
* gender varchar: user gender
* age int: user age
* rank int: rank in this email campaign process
* status varchar: user status in this email campaign process, such as pending, sent or rejected
* "recommendedBeers" list<varchar>: a list of beers we recommended through the email campaign
* check varchar: the manual check status of this user
* "versionId" varchar: the unique id of each feature version
* "timeFeature" varchar: the time feature
* "titleFeature" varchar: the email title or subject line feature
* response varchar: the response action from the user, such as open, reply or interested
* clicks int: number of target actions under a specific version
* visits int: number of users has been tested under a specific version
* rate float: clicks / visits
* "testDate" varchar: the date updating the AB test rate
* "testTime" timestamp: the time updating the AB test rate
* "testRatio" double: the test ratio for that version
