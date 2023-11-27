from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka.kafka_stream import stream_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 10),
    'email': ['npam5499@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG('weather_automation',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
    
    # Set task dependencies
    streaming_task