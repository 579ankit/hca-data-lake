import pandas as pd
import random
import numpy as np
from concurrent import futures
from google.cloud import pubsub_v1
import json
import time
from google.cloud import storage
import helpers.employee_competencies_gen_config as config_file
from datetime import datetime

GCS_URI_PREFIX="gs://"

def change_skill_or_proficiency(emp_skills,all_skills_list):
     
      proficiency_list =["PROF-ID--001","PROF-ID--002","PROF-ID--003"]
      new_all_emp_skills=[]
      current_skills=[]
      choice_gen=[i+1 for i in range(0,len(emp_skills))]
      proficiency_to_update=random.choice(choice_gen)
      counter=1
      for emp_skill in emp_skills:
           
            temp_dict={}
            for k,v in emp_skill.items():
                  if k=="skill_id":
                        current_skills.append(v)
                  if k=='proficiency_id' and counter==proficiency_to_update:
                        v=v[0:len(v)-1]+str(int(v[-1])+1) if int(v[-1])<3 else v
                  temp_dict[k]=v
                 
            counter=counter+1
            new_all_emp_skills.append(temp_dict)


      additional_skills_possible=list(set(all_skills_list)-set(current_skills))
     
      if random.choice(["Yes","No"])=="Yes" and len(additional_skills_possible)!=0:
            new_all_emp_skills.append({"skill_id":random.choice(additional_skills_possible),"proficiency_id":random.choice(proficiency_list)})
           
      return new_all_emp_skills

def update_skills(skills_list,all_skills_list):
    new_skills_list=[]
    for skills in skills_list:
        skills=change_skill_or_proficiency(skills,all_skills_list)
        new_skills_list.append(skills)

    return new_skills_list

def pick_latest_bucket(matching_buckets):

    bucket_name=""
    timestamps=matching_buckets.values()
    latest = max(timestamps)


    for (k,v) in matching_buckets.items():
        if latest==v:
            bucket_name=k
            break


    return bucket_name

def get_bucket_name(bucket_prefix, storage_client):

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

def stream_to_pubsub(employee_competencies_new, project_name, topic_name):

    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=10, # default 100
        max_bytes=1024, # default 1 MB
        max_latency=1, # default 10 ms
    )

    publisher=pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(project_name, topic_name)
    publish_futures = []

    for row_dict in employee_competencies_new.to_dict(orient='records'):
        json_data = json.dumps(row_dict)
        publish_future = publisher.publish(topic_path, data=json_data.encode("utf-8"))
        publish_future.add_done_callback(callback)
        publish_futures.append(publish_future)
        time.sleep(1)


    return futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

def add_inconsistency(employee_competencies_new):

    dict_to_change=employee_competencies_new.tail(1).to_dict('records')[0]
    choice_of_tweak=["emp_id","floor_number","skills"]
    to_tweak=random.choice(choice_of_tweak)

    if to_tweak=="emp_id":
        dict_to_change["emp_id"]= dict_to_change["emp_id"]+str(random.randint(100,200))

    if to_tweak=="floor_number":
        dict_to_change["floor_number"] = random.randint(30,40)

    if to_tweak=="skills":

        skill_or_proficiency=random.choice(["skill","prof"])
        skill_number = str(random.randint(100,200)) if skill_or_proficiency == "skill" else '0'+str(random.randint(10,20))
        proficiency_number = str(random.randint(100,200)) if skill_or_proficiency == "prof" else '00'+str(random.randint(1,3))
        current_skills = dict_to_change["skills"]
        current_skills.append({'skill_id':'SK-ID-'+skill_number,'proficiency_id':'PROF-ID--'+proficiency_number})
        dict_to_change["skills"] = current_skills

    employee_competencies_new = employee_competencies_new[:-1]
    employee_competencies_new.loc[len(employee_competencies_new)] = dict_to_change

    return employee_competencies_new

def get_change(old_skills, new_skills):

    mismatches = []

    for entry1, entry2 in zip(old_skills, new_skills):
        if entry1 != entry2:
            mismatches.append({'skill_id': entry2['skill_id'], 'proficiency_id': entry2['proficiency_id']})

    return mismatches

def get_changed_skills(old_skills_list, new_skills_list):

    additional_skills=[]
    counter = 0
    for new_skills in new_skills_list:
        additional_skills_temp=[]
        
        if len(new_skills)>len(old_skills_list[counter]):
            additional_skills_temp.append(new_skills[-1])
            new_skills = new_skills[:-1]
        
        changed_skills=get_change(old_skills_list[counter], new_skills)
        
        if len(changed_skills)>0:
            additional_skills_temp.append(changed_skills[0])

        additional_skills.append(additional_skills_temp)
        counter=counter+1

    return additional_skills


def employee_competencies_incremental_start_function():

    project_name = config_file.project_name
    storage_client = storage.Client(project=project_name)
    source_bucket_prefix = config_file.backup_repo_bucket_prefix
    source_folder_name=config_file.backup_repo_folder
    topic_name=config_file.pubsub_topic_name
    employee_data_file_prefix = config_file.employee_data_file_prefix
    employee_competencies_file_prefix = config_file.employee_competencies_file_prefix
    floor_skill_association_file_prefix = config_file.floor_skill_association_file_prefix
    output_file_name=config_file.destination_file_name_prefix+".json"

    source_bucket_name=get_bucket_name(source_bucket_prefix,storage_client)
    employee_data_file=get_matching_file_path(project_name, source_bucket_name, source_folder_name, employee_data_file_prefix)
    employee_competencies_file=get_matching_file_path(project_name, source_bucket_name, source_folder_name, employee_competencies_file_prefix)
    floor_skill_association_file=get_matching_file_path(project_name, source_bucket_name, source_folder_name, floor_skill_association_file_prefix)

    employee_df = pd.read_parquet(employee_data_file)
    employee_competencies = pd.read_json(employee_competencies_file)
    floor_skill_association_df=pd.read_csv(floor_skill_association_file)

    emp_id_list=employee_df["emp_id"].to_list()
    sample_for_change=int(len(emp_id_list)*0.005)

    emp_id_with_change=random.sample(emp_id_list,sample_for_change)

    employee_competencies_new=employee_competencies[employee_df.emp_id.isin(emp_id_with_change) == True]
    employee_competencies_new.reset_index(drop=True,inplace=True)
    new_skills_list=employee_competencies_new["skills"].to_list()

    all_skills_df=floor_skill_association_df["skill_id"].drop_duplicates(keep="first").reset_index(drop=True)
    all_skills_list=all_skills_df.to_list()

    new_set_of_skills = update_skills(new_skills_list,all_skills_list)

    employee_competencies_new["skills"]=pd.Series(new_set_of_skills)
    employee_competencies_new["source_timestamp"]=employee_competencies_new["emp_id"].apply(lambda x: datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    employee_competencies = pd.concat([employee_competencies, employee_competencies_new], ignore_index=True)
    employee_competencies.drop_duplicates(subset=["emp_id"],keep='last', ignore_index=True, inplace=True)
    employee_competencies.sort_values(by=['emp_id'],ignore_index=True,inplace=True)

    if len(employee_competencies_new)>0:

        changed_skills = get_changed_skills(new_skills_list,new_set_of_skills)
        employee_competencies_new["skills"] = changed_skills
        employee_competencies_with_errors = add_inconsistency(employee_competencies_new)        
        success=stream_to_pubsub(employee_competencies_with_errors,project_name,topic_name)

        json_list = []
        for index, row in employee_competencies.iterrows():
            skills_list = [i for i in row["skills"]]
            json_record = {"emp_id": row['emp_id'], "floor_number": row['floor_number'], "skills": skills_list,"source_timestamp":row['source_timestamp']}
            json_list.append(json_record)

        output_file_with_bucket=source_folder_name+"/"+output_file_name

        write_as_json(source_bucket_name,output_file_with_bucket, json_list)






