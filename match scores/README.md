# Match Scores

Program Name: Match Scores

Author: Sheng

This program will generate linkedin functions for all contacts loaded and match scores for all job posts. These contacts are based on users' connections. It provides a solution for job application referral systems.

All files can be found at: https://drive.google.com/drive/u/0/folders/0B5wkYHJz9Ns8UGtiRGFSLXVtMm8

Github repository: https://github.com/wsy1607/Machine-Learning-at-BountyMe


## Overview of process
First set up all raw data including a list of fortune 500 companies, a list of top schools and two lists of locations. Then generate linkedin functions for every new contact. Then based on each job post and users' connections, generate match score for each connection (contact) which helps to find the best potential candidates for these job posts.


## Tools and Database Models
Use the python scripts to read and write data into MongoDB, which stores all front-end and back-end data. The following 3 steps include all methods. All methods from step 2 to step 3 are also found in the matchScores project under tasks.py (under celery tasks). Methods in step 1 should be executed only once when setting up all the data infrastructure (they are not included in the celery tasks folder).


### Step 1: Setting up all Raw Data in MongoDB

* insertcompany.py creates a collection of fortune 500 companies.

* insertschool.py creates a collection of top schools.

* insertbroadlocation.py creates a collection of broad locations.

* insertfunctionallocation.py creates a collection of functional locations.


### Step 2: generate linkedin functions for all new contacts

* generatelinkedinfunction.py inserts all new contacts from the remote server into local server as the collection called "profiles". Then generate all linkedin functions for those new contacts.


### Step 3: generate match scores between jobs and contacts

* generatematchscore.py generate match scores between new job posts and contacts. Then from the users' connections (contacts), recommends potential candidates for those job posts based on the match scores.


## Database Reference

* Local: use 3001 as local port for testing for all scripts.

* Remote: use MongoClient at '192.168.18.49'.

* Collections created: "fortune500","topSchool","broadLocation","functionalLocation"


### Important Column References

* "fortune500" list: a list of company names
* "topSchool" list: a list of school names
* "locationName" string: region category
* "locationList" list: a list of states/countries in that region category
* "currentJobTime" int: total months count in the current position
* "yearsOfExperience" string: less than 1 year, 1 to 2 years, 3 to 5 years, 6 to 10 years, more than 10 years
* "companies" list: a list of companies from current to past
* "latestDegree" string: the last degree earned
* "expertiseLevel" int: level of professional expertise
* "seniority" string: student / professor, entry-level, senior-level, manager-level, owner/C-level
