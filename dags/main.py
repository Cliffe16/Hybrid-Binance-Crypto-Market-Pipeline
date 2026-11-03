from airflow.sdk import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
from src.extraction.extract import extract
from src.ingestion.load import load

default_args = {
    'owner': 'Cliffe',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    }

@dag(
    'binance_batch_pipeline',
    default_args=default_args,
    description='Batch extraction and load for the Binance pipeline',
    start_date=datetime(2026, 6, 16),
    schedule='@daily',
    max_active_runs=1,
    catchup=False
    )

def binance_batch_pipeline():
    # Wait for Debezium connector to be active
    wait_for_cdc = HttpSensor(
        task_id='wait_for_debezium',
        http_conn_id='kafka_connect', 
        endpoint='connectors/binance-postgres-connector/status',
        method='GET',
        response_check=lambda response: "RUNNING" in response.text,
        poke_interval=10,
        timeout=300
    )

    @task(task_id='binance_pipeline')
    def main():
        load()

    wait_for_cdc >> main()

binance_batch_pipeline()
