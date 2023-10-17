import random
from datetime import datetime, date
from datetime import timedelta as timedelta
import pandas as pd
import numpy as np
import faker
import ast
import helpers.employee_data_gen_config as config_file
from google.cloud import storage

DATETIME_FORMAT="%Y-%m-%d"
GCS_URI_PREFIX="gs://"

START_DATE=datetime.strptime("2000-01-01", DATETIME_FORMAT)
END_DATE=datetime.strptime("2022-12-01", DATETIME_FORMAT)
START_DATE_YEAR_GRADUATED=datetime.strptime("1990-01-01", DATETIME_FORMAT)
END_DATE_YEAR_GRADUATED=datetime.strptime("1999-12-01", DATETIME_FORMAT)

workplace_environment_file_prefix=config_file.work_environments_file_prefix
project_name=config_file.project_name
bucket_prefix=config_file.bucket_prefix
work_environments_folder_name=config_file.work_environments_folder_name
employee_data_folder_name=config_file.employee_data_folder_name
employment_status_active=config_file.employment_status_active


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

def get_matching_file_path(project_name,bucket_name,work_environments_file_prefix):
    client = storage.Client(project_name)
    for blob in client.list_blobs(bucket_name, prefix=work_environments_folder_name):
        blob_name=blob.name.replace(work_environments_folder_name+"/","")
        matching_blob=blob.name if blob_name.startswith(work_environments_file_prefix) else None
    matching_file_path = None if matching_blob==None else GCS_URI_PREFIX+bucket_name+"/"+matching_blob
    return matching_file_path

def get_workplace_file_path(project_name, workbucket_prefix, work_environments_file_prefix):

    bucket_name=get_bucket_name(project_name, workbucket_prefix)
    workplace_environment_file=get_matching_file_path(project_name,bucket_name,work_environments_file_prefix)
    return bucket_name, workplace_environment_file

def gen_phone():
    first = str(random.randint(100,999))
    second = str(random.randint(1,888)).zfill(3)

    last = (str(random.randint(1,9998)).zfill(4))
    while last in ['1111','2222','3333','4444','5555','6666','7777','8888']:
        last = (str(random.randint(1,9998)).zfill(4))
        
    return '{}-{}-{}'.format(first,second, last)

def gen_email(i, first_name, last_name):
    i=i.replace("EMP-","")
    email="{0}.{1}.{2}@gmail.com".format(first_name, last_name, i)
    return email 

def get_city_state_zipcode(address):
    address=address.split("\n")
    local_address=address[0]
    try:
        city_and_state=address[1].split(",")
        city=city_and_state[0]
        state_and_zip=city_and_state[1].split()
        state=state_and_zip[0]
        zipcode=state_and_zip[1]
    except:
        address_national=address[1].split()
        city=address_national[0]
        state=address_national[1]
        zipcode=address_national[2]
    address_city_state_zipcode={}
    address_city_state_zipcode["local_address"]=local_address
    address_city_state_zipcode["city"]=city
    address_city_state_zipcode["state"]=state
    address_city_state_zipcode["zipcode"]=zipcode
    return address_city_state_zipcode

def gen_last_working_day(employment_status,date_of_joining_company):
    fake=faker.Faker()

    if employment_status in employment_status_active:
        last_working_day=date.today()-timedelta(days=1)

    else:
        last_working_day=fake.date_between_dates(date_start=date_of_joining_company, date_end=datetime.today()-timedelta(weeks=12))
    
    return last_working_day


def employee_data_gen_start_function():

    fake=faker.Faker()

    bucket_name, workplace_environment_file = get_workplace_file_path(project_name, bucket_prefix, workplace_environment_file_prefix)

    workplace_environments_df=pd.read_csv(workplace_environment_file)

    output_df=pd.DataFrame()

    no_of_floors=config_file.no_of_floors
    total_records=config_file.total_records
    job_type=config_file.job_type
    employment_status_inactive=config_file.employment_status_inactive

    floor_list = [i for i in range(1,no_of_floors+1)] 

    employment_status_active=config_file.employment_status_active

    #Add emp_id

    output_df["emp_id"]=np.arange(1,total_records+1)
    output_df["emp_id"]=output_df["emp_id"].apply(lambda x : f"{config_file.emp_id_prefix}-{x:04d}")

    #Add first_name and last_name

    output_df["first_name"]=output_df["emp_id"].apply(lambda x: fake.first_name())
    output_df["last_name"]=output_df["emp_id"].apply(lambda x: fake.last_name())

    #Add SSN, phone, email

    output_df["ssn"]=output_df["emp_id"].apply(lambda x: fake.ssn())
    output_df["phone"]=output_df["emp_id"].apply(lambda x: gen_phone())
    output_df["email"]=output_df.apply(lambda x:gen_email(x.emp_id,x.first_name,x.last_name),axis=1)

    #Add address, district, state, zipcode

    output_df[["address", "district", "state","zipcode"]] = output_df.apply(lambda x: get_city_state_zipcode(fake.address()), axis=1, result_type='expand')

    #Add job_type, job_code, job_code_description

    output_df["job_type"]=output_df["emp_id"].apply(lambda x: random.choice(job_type))

    job_code_df=pd.DataFrame.from_dict({"job_code":config_file.job_code,"job_code_description":config_file.job_code_description})

    output_df["job_code"]=output_df["emp_id"].apply(lambda x: random.choice(config_file.job_code))
    output_df=output_df.merge(job_code_df,how="inner",on="job_code")

    #Add workplace_id, floor_number and department_name

    workplace_ids_list=workplace_environments_df["workplace_id"].to_list()
    output_df["workplace_id"]=output_df["emp_id"].apply(lambda x: random.choice(workplace_ids_list))

    output_df["floor_number"]=output_df["emp_id"].apply(lambda x: random.choice(floor_list))
    output_df["department_name"]=output_df["floor_number"].apply(lambda x: f"{config_file.department_prefix}-{x:04d}")

    #Add date_of_joining_company

    output_df["date_of_joining_company"]=output_df["emp_id"].apply(lambda x: fake.date_between_dates(date_start=START_DATE, date_end=END_DATE))

    #Add employment_status

    employment_status_list=employment_status_active+employment_status_inactive
    employment_status_series=np.random.choice(employment_status_list, total_records, p=['0.83','0.1','0.02','0.05'])
    output_df["employment_status"]=employment_status_series

    #Add manager_id

    workplaces = output_df["workplace_id"].unique()

    managers = {}

    for workplace in workplaces:
        active_employees_in_workplace = output_df[output_df['workplace_id'] == workplace][output_df["employment_status"].isin(employment_status_active)]
        selected_employee = random.choice(active_employees_in_workplace['emp_id'].tolist())
        managers[workplace] = selected_employee

    output_df["manager_id"]=output_df.apply(lambda x: None if x.emp_id in managers.values() else managers[x.workplace_id],axis=1)

    #Add last_working_day

    output_df["last_working_day"]=output_df.apply(lambda x: gen_last_working_day(x['employment_status'],x['date_of_joining_company']),axis=1)

    #Add education_highest_qualification

    output_df["education_highest_qualification"]=output_df["emp_id"].apply(lambda x: random.choice(config_file.education_highest_qualification_list))

    #Add year_graduated

    output_df["year_graduated"]=output_df["emp_id"].apply(lambda x: fake.date_between_dates(date_start=START_DATE_YEAR_GRADUATED, date_end=END_DATE_YEAR_GRADUATED))

    #Add GPA

    output_df["GPA"]=output_df["emp_id"].apply(lambda x: round(fake.pyfloat(min_value=2.0, max_value=4.0),1))

    #Add source_timestamp

    output_df["source_timestamp"]=output_df["date_of_joining_company"].apply(lambda x: datetime.today())

    #Sort Values

    output_df.sort_values(by=['emp_id'],inplace=True)

    #Apply datatime format to relevant fields

    output_df[["date_of_joining_company","last_working_day","year_graduated","source_timestamp"]]=output_df[["date_of_joining_company","last_working_day","year_graduated","source_timestamp"]].apply(pd.to_datetime)

    #Add timestamp to filename

    output_file_name=config_file.destination_file_name_prefix+str(datetime.now().strftime('%Y%m%d%H%M'))+".parquet"
    output_file_with_path=GCS_URI_PREFIX+bucket_name+"/"+employee_data_folder_name+"/"+output_file_name

    output_df.to_parquet(output_file_with_path,index = None,engine='pyarrow',use_deprecated_int96_timestamps=True)
