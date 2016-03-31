#this script updates the status in the central email list
#this script should be executed any time by the process administrator

#note that there are four possible status: preferred, pending, rejected and sent


#load packages
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import pandas as pd


#define the function to check all inputs
def checkinputs(emails,status,centralEmailList):
    for email in emails:
        if email not in centralEmailList["email"].tolist():
            raise ValueError("the input email " + email + " does not exist in the central email list")
    if status not in ["preferred","pending","rejected","sent"]:
        raise ValueError("the input status " + status + " is not valid")

#load the central email list
def loadcentralemails():
    print "retrieving all emails in the central email list as test data from cassandra"
    rawEmailList = session.execute("""
    select * from "centralEmailList"
    """)

    #convert paged results to a list then a dataframe
    centralEmailList = pd.DataFrame(list(rawEmailList))
    return(centralEmailList)

#update email status
def updateemailstatus(emails,status,centralEmailList):
    #update selected users in the central email list for new statuses
    print "update the status in 'centralEmailList' to cassandra, please wait about 1 minute"
    n = len(emails)
    for i in range(n):
        newStatus = status
        email = emails[i]
        prepared_stmt = session.prepare ("""
        UPDATE "centralEmailList" SET status = ? WHERE email = ?
        """)
        bound_stmt = prepared_stmt.bind([newStatus,email])
        stmt = session.execute(bound_stmt)
    print str(n) + " rows of data have been updated"

#main function
if __name__ == '__main__':
    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect('marketingApp')
    session.row_factory = dict_factory

    #below is only for debuging
    #input all emails to change the status
    emails = ["lionrunner73@yahoo.com","matthew.gates@ge.com"]
    #input the status option which we would like those emails to go to
    status = "preferred"

    #load the central email list from cassandra
    centralEmailList = loadcentralemails()
    #check all inputs
    checkinputs(emails,status,centralEmailList)
    #update email status
    updateemailstatus(emails,status,centralEmailList)
