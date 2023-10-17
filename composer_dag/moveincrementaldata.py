import os
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage

bucket_names = ['hca_employee-data_source_20231005', 'hca_hospital-reports_source_20231005']
date_pattern = r'\d{12}'

source_project_id = 'hca-usr-hin-datalake-poc'
destination_project_id = 'hca-usr-hin-landing-datalake'

source_storage_client = storage.Client(project=source_project_id)
destination_storage_client = storage.Client(project=destination_project_id)

def move_and_rename_bucket(bucket_names):
    for source_bucket_name in bucket_names:
        folder_prefixes = []
        if 'employee-data' in source_bucket_name:
            folder_prefixes.append('employee_data/')
        if 'hospital-reports' in source_bucket_name:
            folder_prefixes.append('lab_report_data/')
        
        file_list = get_all_the_files_as_blob(source_bucket_name, folder_prefixes)

        for each_file in file_list:
            each_file_name = each_file.name
            source_file = os.path.splitext(each_file_name)[0]
            date = f"date={extract_date(source_file)}"
            
            if 'employee-data' in source_bucket_name:
                dest_bucket_name = 'hca_employee-data_landing_20231005'
                file_name = each_file_name.replace('employee_data/', '')
                destination_blob_name = f"employee_data/{date}/{file_name}"
            if 'hospital-reports' in source_bucket_name:
                dest_bucket_name = 'hca_hospital-reports_landing_20231005'
                file_name = each_file_name.replace('lab_report_data/', '')
                destination_blob_name = f"lab_report_data/{date}/{file_name}"
            
            source_bucket = source_storage_client.bucket(source_bucket_name)
            destination_bucket = destination_storage_client.bucket(dest_bucket_name)
            new_blob = source_bucket.copy_blob(each_file, destination_bucket, destination_blob_name)


def get_all_the_files_as_blob(source_bucket_name, folder_prefixes):
    file_list = []
    for folder_prefix in folder_prefixes:
        blobs = source_storage_client.list_blobs(source_bucket_name, prefix=folder_prefix, delimiter=None)
        for blob in blobs:
            if not blob.name.endswith('/'):
                file_list.append(blob)
    
    return file_list

def extract_date(source_file):
    match = re.search(date_pattern, source_file)
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
    'move_incremental_data',
    default_args=default_args,
    description='DAG to move and rename files in GCS buckets',
    schedule_interval=None,
)

move_incremental_data_task = PythonOperator(
    task_id='move_incremental_data',
    python_callable=move_and_rename_bucket,
    op_args=[bucket_names], 
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
