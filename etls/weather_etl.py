from datetime import datetime, timedelta
import pandas as pd
from utils.constants import *

def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    return celsius

def transform_load_data(task_instance):
    
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_celsius(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_celsius(data["main"]["temp_max"])
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
                        "Temperature (C)": temp_farenheit,
                        "Feels Like (C)": feels_like_farenheit,
                        "Minimun Temp (C)":min_temp_farenheit,
                        "Maximum Temp (C)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    #print(df_data)
    

    now=datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"s3://{AWS_BUCKET_NAME}/{dt_string}.csv", index=False)