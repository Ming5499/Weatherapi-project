import json
from datetime import datetime
import pandas as pd
import requests


city_name = "hanoi"
base_url = "https://api.openweathermap.org/data/2.5/weather?q="
#Take API key
with open("credentials.txt",  'r') as c:
    api_key = c.read()
    
#https://api.openweathermap.org/data/2.5/weather?q=hanoi&appid=fce40c3ad84e88aba9f09388dc5ca04e    
full_url = base_url + city_name + "&APPID=" + api_key


def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    return celsius

def etl_weather_data(full_url):
    r = requests.get(full_url)
    #Get data
    data = r.json()
    ##print(data)

#{'coord': {'lon': 105.8412, 'lat': 21.0245}, 'weather': [{'id': 502, 'main': 'Rain', 'description': 'heavy intensity rain', 'icon': '10n'}], 'base': 'stations', 'main': {'temp': 305.15, 'feels_like': 312.15, 'temp_min': 305.15, 'temp_max': 305.15, 'pressure': 1002, 'humidity': 75, 'sea_level': 1002, 'grnd_level': 1000}, 'visibility': 10000, 'wind': {'speed': 2.61, 'deg': 82, 'gust': 4.6}, 'rain': {'1h': 8.65}, 'clouds': {'all': 78}, 'dt': 1694257869, 'sys': {'type': 1, 'id': 9308, 'country': 'VN', 'sunrise': 1694212937, 'sunset': 1694257593}, 'timezone': 25200, 'id': 1581130, 'name': 'Hanoi', 'cod': 200}
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data["main"]["temp"])
    feels_like_celsius= kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    #Unix conver to local time
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    #Put in dictionary and Transform
    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (C)": temp_celsius,
                        "Feels Like (C)": feels_like_celsius,
                        "Minimun Temp (C)":min_temp_celsius,
                        "Maximum Temp (C)": max_temp_celsius,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }

    #print(transformed_data)
    #{'City': 'Hanoi', 'Description': 'heavy intensity rain', 'Temperature (C)': 28.0, 'Feels Like (C)': 32.10000000000002, 'Minimun Temp (C)': 28.0, 'Maximum Temp (C)': 28.0, 'Pressure': 1003, 'Humidty': 80, 'Wind Speed': 2.56, 'Time of Record': datetime.datetime(2023, 9, 9, 18, 45, 35), 'Sunrise (Local Time)': datetime.datetime(2023, 9, 9, 5, 42, 17), 'Sunset (Local Time)': datetime.datetime(2023, 9, 9, 18, 6, 33)}

    #Tranform to list and DataFrame
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    #print(df_data)

    #Save to CSV file
    df_data.to_csv("current_weather_data_hanoi.csv", index = False)
    
if __name__ == '__main__':
    etl_weather_data(full_url)