from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
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
    wait_for_cdc = BashOperator(
        task_id='wait_for_debezium',
        bash_command='''
            retries=0
            until curl -s -f http://debezium:8083/connectors/binance-postgres-connector/status | grep -q '"state":"RUNNING"'; do
                if [ $$retries -ge 30 ]; then 
                    echo "Debezium failed to start after 5 minutes. Failing task." >&2; 
                    exit 1; 
                fi
                echo "Waiting for Debezium connector to initialize...";
                sleep 10;
                retries=$$((retries+1))
            done
        '''
        )
    @task(task_id='binance_pipeline')
    def main():
        load()

    wait_for_cdc >> main()

binance_batch_pipeline()
