from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
import pandas as pd
from etls.weather_etl import *


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
