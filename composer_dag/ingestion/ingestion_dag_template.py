import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from helpers.ingest_data import ingest_data

BUCKET_NAME = 'source_bucket_name'  #add source_bucket_name
DESTINATION_BUCKET_NAME = 'destination_bucket_name' #add destionation_bucket_name
FOLDER_PREFIX = 'table_name/' #add the new source/table_name as folder prefix.like: 'employee_data/'
    
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@hourly', #modify based on incremental : @daily, batch : @once or unstructured : @hourly 
}

dag = DAG(
    'batch_ingest_test_specs',  #change the DAG ID
    default_args=default_args,
    description='DAG to move test specs in landing layer',  #change the DAG description
    schedule_interval=None,
)

move_data_task = PythonOperator(
    task_id='batch_ingest_test_specs',  #change the task_id
    python_callable= ingest_data,
    op_args=[BUCKET_NAME, DESTINATION_BUCKET_NAME, FOLDER_PREFIX],
    dag=dag,
)

    
if __name__ == "__main__":
    dag.cli()