from concurrent import futures
from google.cloud import pubsub_v1
import json, logging
import csv
import time
# from helpers.lab_report_data import json_dest_bucket, json_dest_blob
from google.cloud import storage
from helpers.help_funs import get_matching_file_path


project_id = 'hca-usr-hin-datalake-poc'
topic_name = 'hca_lab_report_data_json'
bucket_name="us-central1-hca-datalake-or-34fa5e51-bucket"


batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=10,  # default 100
    max_bytes=1024,  # default 1 MB
    max_latency=1,  # default 10 ms
)


publisher=pubsub_v1.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_id, topic_name)
publish_futures = []


# Resolve the publish future in a separate thread.
def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    print(message_id)


def latest_blob():
    print("get latest file from gcs")




def start_stream():
    # Get latest lab_report_data file
    file_path, bucket_, matching_file=get_matching_file_path("hca-usr-hin-datalake-poc", "test_bucket_09122023", "lab_report_data/json", "lab_report_data_")
    logging.info("File is : ", matching_file)    
    storage_client= storage.Client()


    bucket=storage_client.bucket(bucket_)


    blob=bucket.blob(matching_file)
    blob.download_to_filename("./lab_report_data_temp.json")
    # file=open("/home/airflow/gcs/data/json/lab_report_data_temp.json")
    file=open("./lab_report_data_temp.json")
    data=json.load(file)
    # csvreader = csv.reader(file)
    count=0


    for row in data:
        logging.info(row)
        if count>0:
            row_dict = {
                "lab_report_id": row["lab_report_id"],
                "patient_id": row["patient_id"],
                "test_id": row["test_id"],
                "test_mnemonic": row["test_mnemonic"],
                "test_timestamp": row["test_timestamp"],
                "test_result_value": row["test_result_value"],
                "unit_of_mesurement": row["unit_of_mesurement"],
                "hospital_id": row["hospital_id"],
                "test_type": row["test_type"],
                "source_timestamp": row["source_timestamp"],
                "created_timestamp": row["created_timestamp"],
            }
            json_data = json.dumps(row_dict)
            publish_future = publisher.publish(topic_path, data=json_data.encode("utf-8"))
            publish_future.add_done_callback(callback)
            publish_futures.append(publish_future)
        count+=1
        time.sleep(1)
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)


print(f"Published messages with batch settings to {topic_path}.")



