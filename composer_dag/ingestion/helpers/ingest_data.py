import os
import re
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery

DATE_PATTERN = r'\d{12}'
SOURCE_STORAGE_CLIENT = storage.Client(project='hca-usr-hin-datalake-poc')
DESTINATION_STORAGE_CLIENT = storage.Client(project='hca-usr-hin-landing-datalake')

def ingest_data(BUCKET_NAME, DESTINATION_BUCKET_NAME, FOLDER_PREFIX):
    file_list = get_all_the_files_as_blob(BUCKET_NAME, FOLDER_PREFIX)
    table_name = f"hca_{FOLDER_PREFIX.replace('/', '')}"

    for each_file in file_list:
        each_file_name = each_file.name
        source_file = os.path.splitext(each_file_name)[0]
        date = f"date={extract_date(source_file)}"
        time = f"time={extract_time(source_file)}"
        
        file_name = each_file_name.replace(FOLDER_PREFIX, '')
        destination_blob_name = f"{FOLDER_PREFIX}{date}/{time}/{file_name}"
            
        source_bucket = SOURCE_STORAGE_CLIENT.bucket(BUCKET_NAME)
        destination_bucket = DESTINATION_STORAGE_CLIENT.bucket(DESTINATION_BUCKET_NAME)
        new_blob = source_bucket.copy_blob(each_file, destination_bucket, destination_blob_name)
        each_file.delete()
    if "employee-data" in DESTINATION_BUCKET_NAME:
        sync_biglake_to_bq("hca_employee_data_landing", "hca_employee_managed_table", table_name)
    if "hospital-reports" in DESTINATION_BUCKET_NAME:
        sync_biglake_to_bq("hca_hospital_reports_landing", "hca_hospital_reports_managed_table", table_name)

def sync_biglake_to_bq(landing_dataset, managed_dataset, table_name):
    client = bigquery.client()
    query = f"""
            CREATE OR REPLACE TABLE hca-usr-hin-landing-datalake.{managed_dataset}.{table_name} AS (
            SELECT *
            FROM
            hca-usr-hin-landing-datalake.{landing_dataset}.{table_name}
            );"""
    res = client.query(query).result()

def get_all_the_files_as_blob(source_bucket_name, folder_prefix):
    file_list = []
    blobs = SOURCE_STORAGE_CLIENT.list_blobs(source_bucket_name, prefix=folder_prefix, delimiter=None)
    for blob in blobs:
        if not blob.name.endswith('/'):
            file_list.append(blob)
    
    return file_list

def extract_date(source_file):
    matches = re.finditer(DATE_PATTERN, source_file)
    last_match = None
    for match in matches:
        last_match = match
    if last_match:
        date_string = last_match.group(0)
        year = date_string[:4]
        month = date_string[4:6]
        day = date_string[6:8]
        return f"{year}-{month}-{day}"
    
def extract_time(source_file):
    matches = re.finditer(DATE_PATTERN, source_file)
    last_match = None
    for match in matches:
        last_match = match
    if last_match:
        date_string = last_match.group(0)
        hour = date_string[8:10]
        min = date_string[10:12]
        return f"{hour}:{min}"