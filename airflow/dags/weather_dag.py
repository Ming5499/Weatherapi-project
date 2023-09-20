from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
import pandas as pd


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
    df_data.to_csv(f"s3://weatherapiairlow-anhminh/{dt_string}.csv", index=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 9),
    'email': ['npam5499@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('weather_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

    # Check weather API is available (HttpSensor)
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=hanoi&appid=fce40c3ad84e88aba9f09388dc5ca04e'
    )

    # Extract data from the weather API (HttpOperator)
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=hanoi&appid=fce40c3ad84e88aba9f09388dc5ca04e',
        method='GET',
        response_filter=lambda response: json.loads(response.text),  # Parse JSON response
        log_response=True
    )

    
    transform_load_weather_data = PythonOperator(
        task_id = 'transform_load_weather_data',
        python_callable=transform_load_data
    )
    # Define additional tasks as needed for your workflow

    # Set task dependencies
    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
