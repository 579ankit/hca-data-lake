from concurrent import futures
from google.cloud import pubsub_v1
import json
# import csv
import time
import helpers.employee_competencies_gen_config as config_file
from google.cloud import storage

project_id = config_file.project_name
topic_name = config_file.pubsub_topic_name
GCS_URI_PREFIX="gs://"


def pick_latest_bucket(matching_buckets):

    bucket_name=""
    timestamps=matching_buckets.values()
    latest = max(timestamps)

    for (k,v) in matching_buckets.items():
        if latest==v:
            bucket_name=k
            break

    return bucket_name

def get_bucket_name(project_name, bucket_prefix):

    storage_client = storage.Client(project=config_file.project_name)
    buckets = storage_client.list_buckets()

    matching_buckets={}

    for bucket in buckets:
        bucket_name=bucket.name
        bucket_time_created=bucket.time_created
        if bucket_name.startswith(bucket_prefix):
            matching_buckets[bucket_name]=bucket_time_created

    if len(matching_buckets.keys())>1:
        latest_bucket = pick_latest_bucket(matching_buckets)
        return latest_bucket
        
    elif len(matching_buckets.keys())==1:
        return list(matching_buckets.keys())[0]
    
    elif len(matching_buckets.keys())==0:
        return None

def get_matching_file_path(project_name,bucket_name,folder_name,file_prefix):
    
    client = storage.Client(project_name)
    for blob in client.list_blobs(bucket_name, prefix=folder_name):
        blob_name=blob.name.replace(folder_name+"/","")
        matching_blob=blob.name if blob_name.startswith(file_prefix) else None
    matching_file_path = None if matching_blob==None else GCS_URI_PREFIX+bucket_name+"/"+matching_blob
    return matching_file_path

def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    print(message_id)

def employee_competencies_streamer_start_function():

    storage_client = storage.Client(project=config_file.project_name)

    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=10, # default 100
        max_bytes=1024, # default 1 MB
        max_latency=1, # default 10 ms
    )

    publisher=pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(project_id, topic_name)
    publish_futures = []

    bucket_name=get_bucket_name(project_id, config_file.bucket_prefix)
    employee_competencies_file=get_matching_file_path(project_id, bucket_name, config_file.employee_competencies_folder_name, config_file.destination_file_name_prefix)

    BUCKET = storage_client.get_bucket(bucket_name)
    filename=employee_competencies_file.replace(GCS_URI_PREFIX+bucket_name+"/","")
    blob = BUCKET.get_blob(filename)
    file_data = json.loads(blob.download_as_string())
    # csvreader = csv.reader(file_data)
    count=0
    for row in file_data:
        if count>0:
            row_dict = {
            "emp_id": row["emp_id"],
            "floor_number": row["floor_number"],
            "skills": row["skills"],
            }
            json_data = json.dumps(row_dict)
            publish_future = publisher.publish(topic_path, data=json_data.encode("utf-8"))
            publish_future.add_done_callback(callback)
            publish_futures.append(publish_future)
        count+=1
        time.sleep(1)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

