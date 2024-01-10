import json
import requests
from kafka_producer import get_weather_detail
from kafka import KafkaProducer
import time 
import logging


kafka_bootstrap_servers = 'localhost:9092'

def stream_data():

    current_time = time.time()
    #connecting the broker
    #producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, max_block_ms = 3000)
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms = 3000) 
    
    while True:
        if time.time() > current_time + 60: # 1 minute
            break
        try:
            responese = get_weather_detail()            
                
            #publish and push data
            producer.send('weather',json.dumps(responese).encode('utf-8'))
        except Exception as e:
            logging.ERROR(f'An error occured: {e}')
            continue   
    
    
stream_data()