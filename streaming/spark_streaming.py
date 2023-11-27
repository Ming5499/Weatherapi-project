import logging
from datetime import datetime 
import cassandra
from cassandra.cluster import Cluster
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col 


#create schema
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    
    print("Keyspace created successful!")


def create_table(session):
    
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.weather(
                        cityName STRING,
                        temperature DOUBLE,
                        humidity INT,
                        creationTime TIMESTAMP,
                        
                    );
                    """)
    
    print('Table created successful!')


def insert_data(session,**kwargs):
    
    print('inserting data...')

    cityName = kwargs.get('cityName')
    temperature = kwargs.get('temperature')
    humidity = kwargs.get('humidity')
    creationTime = kwargs.get('creationTime')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(cityName, temperature, humidity, creationTime)
                VALUES (%s, %s, %s, %s, %s)
        """, (cityName, temperature, humidity, creationTime))
        
        logging.info(f"Data inserted successfully")

    except Exception as e:
        logging.error(f'Could not insert data: {e}')


def connect_to_kafka(spark_conn):
    
    spark_df = None
    try: #create the DataFrame
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'weather') \
            .option('startingOffsets', 'earliest') \
            .load() #start reading data from the beginning of the topic and loads the data from Kafka into a Spark DataFrame
            
        logging.info("kafka dataframe created successfully")
        
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df
    
    
def create_spark_connection():
    s_conn = None 
    
    try: #configure Spark to use the Spark Cassandra Connector and the Spark SQL Kafka Connector
        s_conn = SparkSession.builder \
                            .appName('SparkDataStreaming') \
                            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
                            .config('spark.cassandra.connection.host','localhost') \
                            .getOrCreate()            
                            
        s_conn.sparkContext.setLoglevel('ERROR')
        logging.INFO('Spark connection created successful!')
        
    except Exception as e:
        logging.error('Could not create spark session: {e}')

    return s_conn


def create_cassandra_connection():
    
    try:
        #connecting to cassandra cluster 
        cluster = cluster(['localhost'])
        
        cassadra_session = cluster.connect()
        
        return cassadra_session
    
    except Exception as e:
        logging.error('Could not create cassandra connection: {e}')
        return None
    
def create_selection_df_from_kafka(spark_df):
    #Create schema
    schema = StructType([
        StructField("CityName", StringType(), False),
        StructField("Temperature", DoubleType(), False),
        StructField("Humidity", IntegerType(), False),
        StructField("CreationTime", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel    
    
if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()