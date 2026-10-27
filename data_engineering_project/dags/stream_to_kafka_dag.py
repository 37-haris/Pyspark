"""
Airflow DAG: runs the Kafka producer once a day at 01:00.
You can also trigger it manually from the Airflow UI.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from stream_to_kafka import start_streaming

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="random_people_names",
    default_args=default_args,
    schedule="0 1 * * *",   # every day at 01:00
    catchup=False,
) as dag:

    data_stream_task = PythonOperator(
        task_id="kafka_data_stream",
        python_callable=start_streaming,
    )
