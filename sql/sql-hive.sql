CREATE DATABASE IF NOT EXISTS weather_db
USE weather_db

CREATE EXTERNAL TABLE IF NOT EXISTS weather_detail_tbl(
    CityName STRING,
    Temperature DOUBLE,
    Humidity INT,
    CreationTime TIMESTAMP,
    CreationDate DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION 'data/weather_detail'; 
