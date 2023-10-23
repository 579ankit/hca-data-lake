import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery import BigQueryExecuteQueryOperator
from google.cloud import storage
from helpers.ingest_data import ingest_data


BUCKET_NAME = 'hca-data-lake-poc_unstructured_data'  #add source_bucket_name
FOLDER_PREFIX = 'table_name/' #add the new source/table_name as folder prefix.like: 'employee_data/'
    
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_dag_workflow',  #change the DAG ID
    default_args=default_args,
    description='test_dag_workflow',  #change the DAG description
    schedule_interval=None, #modify based on incremental : @daily, batch : @once or unstructured : @hourly ,
)

move_data_task = PythonOperator(
    task_id='test_dag_workflow',  #change the task_id
    python_callable= ingest_data,
    op_args=[BUCKET_NAME, FOLDER_PREFIX],
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()