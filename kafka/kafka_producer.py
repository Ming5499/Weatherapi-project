from kafka import KafkaProducer
import requests
import json
import time
from utils.constants import *
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_name = 'weather_test'

producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    return celsius

def get_weather_detail(full_url):
    r = requests.get(full_url)
    data = r.json()
    city_name = data["name"]
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    humidity = data["main"]["humidity"]
    json_message = {'CityName': city_name, 'Temperature': temp_celsius, 'Humidity': humidity, 'CreatedAt': time.strftime("%Y-%m-%d %H:%M:%S")}
    return json_message



base_url = "https://api.openweathermap.org/data/2.5/weather?q="
api_key = CREDENTIAL

def kafka_stream():
    while True:
        city_name = "hanoi"
        full_url = base_url + city_name + "&APPID=" + api_key
        json_message = get_weather_detail(full_url)
        producer.send(kafka_topic_name, json_message)
        print('Published message 1: ' + json.dumps(json_message))
        print('Wait for 5 seconds ...')
        time.sleep(5)

        city_name = "danang"
        full_url = base_url + city_name + "&APPID=" + api_key
        json_message = get_weather_detail(full_url)
        producer.send(kafka_topic_name, json_message)
        print('Published message 2: ' + json.dumps(json_message))
        print('Wait for 5 seconds ...')
        time.sleep(5)

        city_name = "hue"
        full_url = base_url + city_name + "&APPID=" + api_key
        json_message = get_weather_detail(full_url)
        producer.send(kafka_topic_name, json_message)
        print('Published message 3: ' + json.dumps(json_message))
        print('Wait for 5 seconds ...')
        time.sleep(5)


kafka_stream()