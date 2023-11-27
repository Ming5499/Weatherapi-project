CREATE EXTERNAL TABLE weather_detail_tbl(
    CityName STRING,
    Temperature DOUBLE,
    Humidity INT,
    CreationTime TIMESTAMP,
    CreationDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'data/weather_detail';