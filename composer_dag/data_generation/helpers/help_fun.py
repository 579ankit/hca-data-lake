from google.cloud import storage


def pick_latest_file(matched_blobs):


    latest_file=""
    latest=max(list(matched_blobs.values()))
    for k,v in matched_blobs.items():
        if v == latest:
            latest_file=k
    return latest_file


def get_matching_file_path(project_name,bucket_name,folder_name,employee_data_file_prefix):
    client = storage.Client(project_name)
    matched_blobs={}
    for blob in client.list_blobs(bucket_name, prefix=folder_name):
        blob_name=blob.name.replace(folder_name+"/","")
        if blob_name.startswith(employee_data_file_prefix):
            matched_blobs[blob.name] = blob.time_created
     
    matching_file = pick_latest_file(matched_blobs)
    matching_file_path = "gs://"+bucket_name+"/"+matching_file
    return matching_file_path, bucket_name, matching_file


# str_=get_matching_file_path("hca-usr-hin-datalake-poc", "us-central1-hca-datalake-or-34fa5e51-bucket", "data/csvs", "lab_report_data_")
# print(str_)



