import pandas as pd
import random
from datetime import datetime
import helpers.employee_data_gen_config as config_file
from google.cloud import storage

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

def pick_oldest_file(matched_blobs):

    oldest_file=""
    oldest=min(list(matched_blobs.values()))
    for k,v in matched_blobs.items():
        if v == oldest:
            oldest_file=k
    return oldest_file

def get_matching_file_path(project_name,bucket_name,folder_name,employee_data_file_prefix):
    client = storage.Client(project_name)
    matched_blobs={}
    for blob in client.list_blobs(bucket_name, prefix=folder_name):
        blob_name=blob.name.replace(folder_name+"/","")
        if blob_name.startswith(employee_data_file_prefix):
            matched_blobs[blob.name] = blob.time_created
     
    matching_file = pick_oldest_file(matched_blobs)
    matching_file_path = GCS_URI_PREFIX+bucket_name+"/"+matching_file
    return matching_file_path

def gen_phone():
    first = str(random.randint(100,999))
    second = str(random.randint(1,888)).zfill(3)

    last = (str(random.randint(1,9998)).zfill(4))
    while last in ['1111','2222','3333','4444','5555','6666','7777','8888']:
        last = (str(random.randint(1,9998)).zfill(4))
        
    return '{}-{}-{}'.format(first,second, last)

def change_email(current_email):
    email_username=current_email.split("@")[0]
    email_domain=current_email.split("@")[1]
    email_username_new=email_username.replace(email_username.split('.')[2],str(datetime.today().strftime("%y%d%m")))
    new_email = email_username_new + "@" + email_domain
    return new_email

def employee_data_gen_incremental_start_function():

    project_name=config_file.project_name
    bucket_prefix=config_file.source_bucket_prefix
    employee_data_folder_name=config_file.employee_data_folder_name
    employee_data_file_prefix=config_file.destination_file_name_prefix

    bucket_name=get_bucket_name(project_name,bucket_prefix)
    matched_file=get_matching_file_path(project_name,bucket_name,employee_data_folder_name,employee_data_file_prefix)
    employee_df=pd.read_parquet(matched_file)
    emp_id_list=employee_df["emp_id"].to_list()

    sample_for_change=int(len(emp_id_list)*0.01)

    emp_id_phone_numbers_change=random.sample(emp_id_list,sample_for_change)
    emp_id_email_change=random.sample(emp_id_list,sample_for_change)
    emp_id_with_change=list(set(emp_id_phone_numbers_change+emp_id_email_change))

    employee_df_new=employee_df[employee_df.emp_id.isin(emp_id_with_change) == True]
    employee_df_new.reset_index(drop=True,inplace=True)

    employee_df_new["phone"]=employee_df_new["emp_id"].apply(lambda x: gen_phone() if x in emp_id_phone_numbers_change else x)
    employee_df_new["email"]=employee_df_new.apply(lambda x: change_email(x.email) if x.emp_id in emp_id_email_change else x.email,axis=1)
    employee_df_new["source_timestamp"]=employee_df_new["emp_id"].apply(lambda x: datetime.today())

    employee_df_new[["date_of_joining_company","last_working_day","year_graduated","source_timestamp"]]=employee_df_new[["date_of_joining_company","last_working_day","year_graduated","source_timestamp"]].apply(pd.to_datetime)

    destination_bucket_name=get_bucket_name(project_name,config_file.bucket_prefix)   
    output_file_name=config_file.destination_file_name_prefix+str(datetime.now().strftime('%Y%m%d%H%M%S'))+".parquet"
    output_file_with_path=GCS_URI_PREFIX+destination_bucket_name+"/"+config_file.destination_folder_name+"/"+output_file_name

    employee_df_new.to_parquet(output_file_with_path,index = None,engine='pyarrow',use_deprecated_int96_timestamps=True)




