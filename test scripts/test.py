#this script helps to generate some fake data to complete an email campaign cycle for testing purposes

#there are three sessions
#session1: drop all existing Tables
#session2: generate test manual check feedback
#session3: generate test use response


#load packages
from cassandra.cluster import Cluster
import pandas as pd
import time
import csv
from datetime import datetime


#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect('marketingApp')

#session1: drop all tables and reset everything in cassandra
# session.execute("""
# drop table "rawSalesData"
# """)
# session.execute("""
# drop table "rawEmailList"
# # """)
# session.execute("""
# drop table "rawVersionInfo"
# """)
# session.execute("""
# drop table "centralEmailList"
# """)
# session.execute("""
# drop table "proposeEmailList"
# """)
# session.execute("""
# drop table "finalEmailList"
# """)
# session.execute("""
# drop table "sentEmailList"
# """)
# session.execute("""
# drop table "versionTests"
# """)

#session2: update check status

#round1:
# session.execute("""
# update "proposeEmailList" set check = 'no' where email in ('shertzogovina@gmail.com','roderick.lawrence@gmail.com','leighs713@gmail.com','patpsenak@gmail.com','reidyj13@gmail.com')
# """)
# session.execute("""
# update "proposeEmailList" set check = 'pending' where email in ('evan.maltese@ey.com','mabamba@live.com','paine_tim@yahoo.com','michael.cusey@gmail.com','chouston0106@gmail.com')
# """)

#round2:
# session.execute("""
# update "proposeEmailList" set check = 'no' where email in ('steve@thechiefgood.com','darryl@lyrrad.net','brendabaldassano@hotmail.com','joseph.ko@gmail.com','stevenwork00@gmail.com','reidyj13@gmail.com','jezwinmurphy@gmail.com','ethancf@gmail.com','haneyboy1@gmail.com','neil.dbh@gmail.com')
# """)
# session.execute("""
# update "proposeEmailList" set check = 'pending' where email in ('bristolr@primelineinc.com','almondjoy170@gmail.com','lr3@airsystems-inc.com','nvmuddin00@hotmail.com','ablarsn@gmail.com','ashley.hornedo@yahoo.com','matt@mutedwarf.com','uacats1997@yahoo.com','igorg0505@gmail.com','jamiecorvino@hotmail.com')
# """)

#session3: get email responses

#round1:
session.execute("""
update "sentEmailList" set response = 'open' where email in ('joshua.white@vacationclub.com','skolanach@gmail.com','Paul@Withun.com','jason.garton@gmail.com','derekrand0433@hotmail.com','tnytatt@yahoo.com','stkforsale@aol.com','ckenne2@gmail.com','ccalderonm@gmail.com','mportnoy@gmail.com')
""")
session.execute("""
update "sentEmailList" set response = 'click' where email in ('nt99go@gmail.com','millermm99@yahoo.com','swoopjones@aol.com','mattpayne@phslegal.com','msagrati202@comcast.net')
""")
session.execute("""
update "sentEmailList" set response = 'sold' where email in ('steamenginevapes@gmail.com','cgilgan@gmail.com','montedia@pla.net.py','alexzcool@hotmail.com','mbright317@hotmail.com')
""")

#round2:
# session.execute("""
# update "sentEmailList" set response = 'open' where email in ('shertzogovina@gmail.com','roderick.lawrence@gmail.com','patpsenak@gmail.com','reidyj13@gmail.com','paine_tim@yahoo.com','chouston0106@gmail.com','edm810@yahoo.com','mineral207@aol.com','brwesbury0@gmail.com','fbachschmidt@mac.com'
# )
# """)
# session.execute("""
# update "sentEmailList" set response = 'click' where email in ('rocminrl@rochester.rr.com','ctrimborn@wi.rr.com','jeffmart22@yahoo.com','jhaston26@yahoo.com','jespi3@gmail.com')
# """)
# session.execute("""
# update "sentEmailList" set response = 'sold' where email in ('ezra.w.perkins@gmail.com','golf4collet@gmail.com','gambelli6@yahoo.com','kyleetrotter@gmail.com','wood.patrick@gmail.com')
# """)
