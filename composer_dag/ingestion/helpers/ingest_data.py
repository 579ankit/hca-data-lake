import os
import re
from datetime import datetime, timedelta
from google.cloud import storage

DATE_PATTERN = r'\d{12}'
SOURCE_STORAGE_CLIENT = storage.Client(project='hca-usr-hin-datalake-poc')
DESTINATION_STORAGE_CLIENT = storage.Client(project='hca-usr-hin-landing-datalake')

def ingest_data(BUCKET_NAME, DESTINATION_BUCKET_NAME, FOLDER_PREFIX):
    file_list = get_all_the_files_as_blob(BUCKET_NAME, FOLDER_PREFIX)

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
    
def extract_time(source_file):
    match = re.search(DATE_PATTERN, source_file)
    if match:
        date_string = match.group(0)
        hour = date_string[8:10]
        min = date_string[10:12]
        return f"{hour}:{min}"