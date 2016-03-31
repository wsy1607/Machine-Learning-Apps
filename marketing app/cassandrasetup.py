#this script should be executed for basic setting up for Cassandra
#note that we should re-define the strategy method and number of replicas later

#load packages
from cassandra.cluster import Cluster

#main function
if __name__ == '__main__':
    #connect to cassandra
    print "connecting to cassandra for local mode"
    cluster = Cluster()
    session = cluster.connect()
    #create a keyspace called "marketingApp" with simple strategy and 3 replicas
    session.execute("""
        CREATE KEYSPACE "marketingApp" WITH replication
            = {'class':'SimpleStrategy', 'replication_factor':3};
    """)
    print "key space 'marketingApp' has been defined."
