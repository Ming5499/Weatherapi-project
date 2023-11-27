import logging
from datetime import datetime 
import cassandra
from cassandra.cluster import Cluster


#create schema
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    
    print("Keyspace created successful!")


def create_table(session):
    
    session.execute("""
                    CREATE TABLE IF NOT EXISTS (
                    );
                    """)
    
    print('Table created successful!')


def create_cassandra_connection():
    
    try:
        #connecting to cassandra cluster 
        cluster = cluster(['localhost'])
        
        cassadra_session = cluster.connect()
        
        return cassadra_session
    
    except Exception as e:
        logging.error('Could not create cassandra connection: {e}')
        return None

session = create_cassandra_connection()
create_keyspace(session)
create_table(session)