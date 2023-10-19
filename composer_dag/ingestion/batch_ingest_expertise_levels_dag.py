import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from helpers.ingest_data import ingest_data

BUCKET_NAME = 'hca_employee-data_source_20231005'
DESTINATION_BUCKET_NAME = 'hca_employee-data_landing_20231005'
FOLDER_PREFIX = 'expertise_levels/'
    
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}

dag = DAG(
    'batch_ingest_expertise_levels',
    default_args=default_args,
    description='DAG to move expertise levels in landing layer',
    schedule_interval=None,
)

move_incremental_data_task = PythonOperator(
    task_id='batch_ingest_expertise_levels',
    python_callable= ingest_data,
    op_args=[BUCKET_NAME, DESTINATION_BUCKET_NAME, FOLDER_PREFIX], 
    dag=dag,
)

    
if __name__ == "__main__":
    dag.cli()