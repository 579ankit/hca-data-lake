import os, logging
from os import listdir
import shutil, csv
from google.cloud import storage
from helpers.help_funs import get_matching_file_path
from datetime import datetime as dt


# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "CREDENTIALS.json"
bucket_name = "hca_hospital-reports_source_20231005"

public_uris=[]


def get_blobs(num_images):
    source_files=[]
    client = storage.Client()
    blobs=client.list_blobs(bucket_name, prefix='data/source_lab_report_images')
    for image in blobs:
        print("*********")
        source_files.append(image.download_as_string())
        num_images+=1
    return source_files
   
def get_lab_test_details():
    file_path, bucket_, matching_file=get_matching_file_path("hca-usr-hin-datalake-poc", "test_bucket_09122023", "lab_report_data/csvs", "lab_report_data_")
    client = storage.Client()
    num_images=100
    # print("file to be used : {} / {}".format(bucket_, matching_file))
    source_files=get_blobs(num_images)
    # print("Number of images in source : ", len(source_files))
    count=0
    bucket=client.bucket(bucket_)
    blob=bucket.blob(matching_file)
    blob.download_to_filename("./lab_report_data_temp.csv")
    # print(blob.name)
    with open("./lab_report_data_temp.csv", 'r') as file:
        csvreader = csv.reader(file)
        bucket = client.bucket(bucket_name)
        for row in csvreader:
            if row[2]!='test_id' and count<num_images:
                lab_report_id=row[0]
                # test_date=row[3]
                test_date=dt.now().strftime("%Y%m%d%H%M")
                source_file=source_files[(count % len(source_files))]
                destination_file = 'lab_report_object_data/{}_{}.png'.format(lab_report_id, test_date)
                count+=1
                count%=num_images
                destination_blob_name = destination_file
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_string(source_file)
                # print(blob.name)
                public_uris.append(blob.public_url)
                # print(blob.public_url)







