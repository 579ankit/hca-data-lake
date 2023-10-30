import os, logging
from os import listdir
import shutil, csv
from google.cloud import storage
from helpers.help_funs import get_matching_file_path
from datetime import datetime as dt


# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "hca-data-lake-poc-ce059e6320a2.json"
bucket_name = "hca_hospital-reports_source_20231005"
# bucket_name="us-central1-hca-datalake-or-34fa5e51-bucket"


public_uris=[]



def get_blobs(num_images):
    source_files=[]
    client = storage.Client()
    blobs=client.list_blobs(bucket_name, prefix='data/source_lab_report_images')
    for image in blobs:
        print("*********")
        source_files.append(image.download_as_string())
        # source_files.append(image.download_to_filename("./img1.png"))
        num_images+=1
    return source_files
   
def get_lab_test_details():
    file_path, bucket_, matching_file=get_matching_file_path("hca-usr-hin-datalake-poc", "test_bucket_09122023", "lab_report_data/csvs", "lab_report_data_")
    client = storage.Client()
    num_images=100
    print("file to be used : {} / {}".format(bucket_, matching_file))
    source_files=get_blobs(num_images)
    print("INSIDE get_lab_test_details :")
    print("Number of images in source : ", len(source_files))
    count=0
    bucket=client.bucket(bucket_)
    blob=bucket.blob(matching_file)
    blob.download_to_filename("./lab_report_data_temp.csv")
    print(blob.name)
    print("Looks good till now.....!")
    with open("./lab_report_data_temp.csv", 'r') as file:
        logging.info("Inside with clause......!")
        csvreader = csv.reader(file)
        bucket = client.bucket(bucket_name)
        for row in csvreader:
            if row[2]!='test_id' and count<num_images:
                logging.info(row)
                lab_report_id=row[0]
                # test_date=row[3]
                test_date=dt.now().strftime("%Y%m%d%H%M")
                source_file=source_files[(count % len(source_files))]
                destination_file = 'lab_report_object_data/{}_{}.png'.format(lab_report_id, test_date)
                # Copy the file
                # shutil.copy2(source_file, destination_file)
                count+=1
                count%=num_images
                destination_blob_name = destination_file
                blob = bucket.blob(destination_blob_name)
                # blob.upload_from_filename(destination_file)
                # blob.upload_from_filename(source_file)
                blob.upload_from_string(source_file)
                print(blob.name)
                public_uris.append(blob.public_url)
                print(blob.public_url)
                print("-----------------------")


# get_lab_test_details()


Columns=["lab_report_uri", "metadata_1", "metadata_2"]
data=[]
for i in public_uris:
    lab_report_uri=i
    metadata_1="metadata-1"
    metadata_2="metadata-2"
    row={
        "lab_report_uri": lab_report_uri,
        "metadata_1": metadata_1,
        "metadata_2": metadata_2,
    }
    data.append(row)


# # Write the data to a CSV file
# with open("./data/lab_report_object_table.csv", "w", newline='') as f:
#     writer = csv.DictWriter(f, fieldnames=Columns)
#     writer.writeheader()
#     writer.writerows(data)



