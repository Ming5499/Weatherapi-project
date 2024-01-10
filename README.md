# Weatherapi project
Weather Data Streaming and Processing Pipeline
This project implements a streaming data pipeline to collect weather information from an external API, process it using Apache Spark Structured Streaming, and store the data in both Apache Hive and Hadoop Distributed File System (HDFS).

Features
Data Sources: Fetches weather data from an external API (e.g., OpenWeatherMap).
Technologies Used: Apache Kafka for data ingestion, Apache Spark Structured Streaming for data processing, Apache Hive for data storage, and HDFS for intermediate data storage.
Programming Languages: Utilizes Scala and Python for implementing Spark-based applications.
Project Components
Producer Script :

Collects weather data from an external API.
Publishes the data to a Kafka topic named 'weather'.
Consumer Script :

Reads data from the Kafka topic 'weather' using Spark Structured Streaming.
Processes the data (e.g., formats, manipulates).
Stores the processed data into a Cassandra database table named 'weather'.
Scala Application :

Reads streaming data from the Kafka topic 'weather' using Spark Structured Streaming.
Processes the data (e.g., transformations).
Writes processed data to the console and HDFS in CSV format.
Saves the processed data into an Apache Hive table named 'weather_detail_tbl' in the 'weather_database'.
Setup Instructions
Prerequisites:

Apache Kafka, Apache Spark, Apache Cassandra, and Apache Hive must be installed and properly configured.
Execution Steps:

Execute the producer script (streaming_weather_data.py) to start collecting and publishing weather data to Kafka.
Run the consumer script (cassandra_spark_integration.py) to process and store data into Cassandra.
Run the Scala application (WeatherStreamingDemo.scala) to process data and save it into Hive and HDFS.
Configuration:

Update configuration files and connection details as needed in the scripts for proper functionality (e.g., Kafka server details, API keys, file paths).
Note:

Ensure the correct setup and configuration of Hive and HDFS directories for data storage.
Conclusion
This pipeline enables real-time data streaming, processing, and storage of weather information using a combination of Apache Kafka, Spark Structured Streaming, Cassandra, and Hive. Adjustments can be made based on specific use cases and further scalability requirements.
