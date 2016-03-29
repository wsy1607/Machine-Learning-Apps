# Beer Recommendations

Program Name: Beer Recommendations

Author: Sheng

This program will generate similar beers and recommended beer boxes as a part of our e-commerce product.

All files can be found at: https://drive.google.com/drive/u/0/folders/0B5wkYHJz9Ns8UGtiRGFSLXVtMm8

Github repository: https://github.com/wsy1607/Marketing-App


## Overview of process
First set up all raw data. Then generate similar beers and recommended beer boxes using sales and user profiles data. Update ratings by web traffic logs (haven't finished). Please note that there are some randomly simulated parameters which haven't been well-defined (can be found with python function random()) so far. Also, several tuning parameters, for example, labeled as n, m and k should be re-defined if necessary.


## Tools and Database Models
Use the python scripts to read and write data into MongoDB, which stores all front-end and back-end data. The following 4 steps include all methods. All methods from step 2 to step 4 are found in the beerRecommendation project under tasks.py (under celery tasks). Methods in step 1 should be executed only once when setting up all the data infrastructure (they won't show up in the celery tasks folder).


### Step 1: Setting up all Raw Data in MongoDB

* insertsales.py creates a collection of beers with sales

* insertlocationreference.py creates a collection of location information


### Step 2: generate similar beers using k-means clustering methods

* similarbeers.py queries all existing beers and sales data from collections "beers" and "rawSalesData" and add similar beers to the "beers" collection.


### Step 3: generate proposed beer boxes and shipped beer boxes for club members

* generateproposebox.py generates proposed boxes consist of six beers for all club members using their profiles and sales data.

* generatesingleproposebox.py generates one proposed box for one specific club member using their profiles and sales data.

* editsingleproposebox.py edits one proposed box given certain requirements.

* generateshippedbox.py generates shipped boxes consist of six beers for all club members using their profiles, sales data and ratings of proposed boxes.

* generatesingleshippedbox.py generates one shipped box for one specific club member using their profiles, sales data and ratings of proposed boxes.

* editsingleshippedbox.py edits one shipped box given certain requirements.


### Step 4 (not finished yet): Update beers ratings which are associated with ranks displayed in the general search pages.

* ratings.py updates all beers ratings by front-end web traffic logs.

## Database Reference

* Local: use 3001 for local port testing for all scripts

* Collections created: "rawSalesData","locationReference"

### Important Column References

* "productTitle" string: the title of the product
* "vendor" string: the vendor of the product
* "quantityCount" int: total quantity count of this order
* "price" float: price
* "totalSales" float: total sales of this order
* "shippingCity" string: city in the shipping address
* "sku" string: combined title of the product
* "trafficSource" string: traffic source
* "variantTitle" string: size info
* "name" string: name
* "region" string: region
* "productType" string: the type of the product
* "shippingProvince" string: state in the shipping address
* "email" string: email
* "locationName" string: same as region
* "locationList" list: a list of states/countries in that region category
