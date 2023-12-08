import pandas as pd
import json
import random
import faker
from datetime import datetime
from google.cloud import storage
import helpers.employee_competencies_gen_config as config_file
import time
from concurrent import futures
from google.cloud import pubsub_v1
import json

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

    storage_client = storage.Client(project=project_name)
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
    matching_blob=None
    for blob in client.list_blobs(bucket_name, prefix=folder_name):
        blob_name=blob.name.replace(folder_name+"/","")
        if blob_name.startswith(file_prefix):
            matching_blob=blob.name
            break
    matching_file_path = None if matching_blob==None else GCS_URI_PREFIX+bucket_name+"/"+matching_blob
    return matching_file_path

def gen_random_skills(floor_number,floor_skills_dict,proficiency_list):
    total_floors=floor_skills_dict.keys()
    skill_set=[]
    for i in total_floors:
        if floor_number==1:
            skills=random.sample(floor_skills_dict[i],random.randint(1,3))
            skill_set.append(skills)
        else:
            skills=random.sample(floor_skills_dict[i],random.randint(0,2))
            if len(skills)!=0:
               skill_set.append(skills)

    skill_set = sum(skill_set, [])
    skill_set_dict = {p: random.choice(proficiency_list) for p in skill_set}
    return skill_set_dict

def write_as_json(bucket_name,output_file_with_bucket, json_list):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_file_with_bucket)
    blob.upload_from_string(
    data=json.dumps(json_list,indent=2),
    content_type='application/json'
    )

def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    print(message_id)

def stream_to_pubsub(project_name,json_list):

    batch_settings = pubsub_v1.types.BatchSettings(
        max_bytes=1024, # default 1 MB
        max_latency=1, # default 10 ms
    )

    publisher=pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(project_name, config_file.pubsub_topic_name)
    publish_futures = []

    for item in json_list:
        json_data = json.dumps(item)
        publish_future = publisher.publish(topic_path, data=json_data.encode("utf-8"))
        publish_future.add_done_callback(callback)
        publish_futures.append(publish_future)
        time.sleep(0.02)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

def employee_competencies_gen_start_function():
    employee_data_folder_name=config_file.employee_data_folder_name
    floor_skill_association_folder_name=config_file.floor_skill_association_folder_name
    employee_data_file_prefix=config_file.employee_data_file_prefix
    floor_skill_association_file_prefix=config_file.floor_skill_association_file_prefix
    project_name = config_file.project_name

    bucket_name =get_bucket_name(project_name,config_file.source_bucket_prefix)

    employee_data_file=get_matching_file_path(project_name,bucket_name,employee_data_folder_name,employee_data_file_prefix)
    floor_skill_association_file=get_matching_file_path(project_name,bucket_name,floor_skill_association_folder_name,floor_skill_association_file_prefix)

    employee_df=pd.read_parquet(employee_data_file)
    floor_skill_association_df=pd.read_csv(floor_skill_association_file)

    output_df=pd.DataFrame()

    floor_skill_unique_df=pd.DataFrame()
    output_df[["emp_id","floor_number"]]=employee_df[["emp_id","floor_number"]]

    floor_skill_unique_df=floor_skill_association_df[["floor_number","skill_id"]].drop_duplicates(keep="first").reset_index(drop=True)

    floor_skill_dict = floor_skill_unique_df.groupby('floor_number')['skill_id'].apply(list).to_dict()
    proficiency_list=floor_skill_association_df["proficiency_id"].drop_duplicates(keep="first").reset_index(drop=True)

    output_df["skills"]=output_df.apply(lambda x: gen_random_skills(x.floor_number,floor_skill_dict,proficiency_list),axis=1)
    output_df["source_timestamp"]=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Convert DataFrame to the desired JSON format
    json_list = []
    for index, row in output_df.iterrows():
        skills_list = [{"skill_id": skill, "proficiency_id": prof} for skill, prof in row['skills'].items()]
        json_record = {"emp_id": row['emp_id'], "floor_number": row['floor_number'], "skills": skills_list,"source_timestamp":row['source_timestamp']}
        json_list.append(json_record)

    output_file_name=config_file.destination_file_name_prefix+str(datetime.now().strftime('%Y%m%d%H%M%S'))+".json"
    output_file_with_bucket=config_file.employee_competencies_folder_name+"/"+output_file_name
    # output_file_with_path=GCS_URI_PREFIX+bucket_name+"/"+config_file.employee_competencies_folder_name+"/"+output_file_name
    output_bucket_name=get_bucket_name(project_name,config_file.bucket_prefix)
    write_as_json(output_bucket_name,output_file_with_bucket, json_list)
    #Add backup
    output_file_bkp_with_bucket=config_file.backup_repo_folder+"/"+config_file.destination_file_name_prefix+".json"
    write_as_json(bucket_name,output_file_bkp_with_bucket, json_list)
    #stream to pubsub
    stream_to_pubsub(project_name,json_list)