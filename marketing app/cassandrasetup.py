#this script should be executed for basic setting up for Cassandra


#load packages
from cassandra.cluster import Cluster

#connect to cassandra
print "connecting to cassandra for local mode"
cluster = Cluster()
session = cluster.connect()
session.execute("""
    CREATE KEYSPACE "marketingApp" WITH replication
        = {'class':'SimpleStrategy', 'replication_factor':3};
""")
print "key space 'marketingApp' has been defined."
