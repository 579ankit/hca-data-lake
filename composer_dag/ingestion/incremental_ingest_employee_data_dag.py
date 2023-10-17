import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage

BUCKET_NAME = 'hca_employee-data_source_20231005'
DESTINATION_BUCKET_NAME = 'hca_employee-data_landing_20231005'
DATE_PATTERN = r'\d{12}'
FOLDER_PREFIX = 'employee_data/'

SOURCE_STORAGE_CLIENT = storage.Client(project='hca-usr-hin-datalake-poc')
DESTINATION_STORAGE_CLIENT = storage.Client(project='hca-usr-hin-landing-datalake')

def ingest_incremental_data(bucket_name):

    file_list = get_all_the_files_as_blob(bucket_name, FOLDER_PREFIX)

    for each_file in file_list:
        each_file_name = each_file.name
        source_file = os.path.splitext(each_file_name)[0]
        date = f"date={extract_date(source_file)}"
        
        file_name = each_file_name.replace(FOLDER_PREFIX, '')
        destination_blob_name = f"{FOLDER_PREFIX}{date}/{file_name}"
            
        source_bucket = SOURCE_STORAGE_CLIENT.bucket(bucket_name)
        destination_bucket = DESTINATION_STORAGE_CLIENT.bucket(DESTINATION_BUCKET_NAME)
        new_blob = source_bucket.copy_blob(each_file, destination_bucket, destination_blob_name)

def get_all_the_files_as_blob(source_bucket_name, folder_prefix):
    file_list = []
    blobs = SOURCE_STORAGE_CLIENT.list_blobs(source_bucket_name, prefix=folder_prefix, delimiter=None)
    for blob in blobs:
        if not blob.name.endswith('/'):
            file_list.append(blob)
    
    return file_list

def extract_date(source_file):
    match = re.search(DATE_PATTERN, source_file)
    if match:
        date_string = match.group(0)
        year = date_string[:4]
        month = date_string[4:6]
        day = date_string[6:8]
        return f"{year}-{month}-{day}"
    
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'incremental_ingest_employee_data',
    default_args=default_args,
    description='DAG to move employee data in landing layer',
    schedule_interval=None,
)

move_incremental_data_task = PythonOperator(
    task_id='incremental_ingest_employee_data',
    python_callable= ingest_incremental_data,
    op_args=[BUCKET_NAME], 
    dag=dag,
)

    
if __name__ == "__main__":
    dag.cli()