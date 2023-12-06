import csv
import faker
import random, logging
import json, copy
# import avro


# from avro.datafile import DataFileWriter, DataFileReader
# from avro.io import DatumWriter, DatumReader


from datetime import datetime
from helpers.data_lists import hospitals, hospitals_addresses
from dateutil.relativedelta import relativedelta
from google.cloud import storage


# Create a Faker object
fake = faker.Faker()


Columns=["hospital_id", "hospital_name", "hospital_address", "hospital_district", "hospital_state", "hospital_zipcode", "source_timestamp", "created_timestamp", "updated_timestamp"]




def generate_hospital_id(number):
    emp_id_prefix = "HOS"
    emp_id_counter = number
    while True:
        if number%4==0:
            yield ""
        else:
            yield f"{emp_id_prefix}-{emp_id_counter:04d}"
        # yield f"{emp_id_prefix}-{emp_id_counter:04d}"
        emp_id_counter += 1


def get_city_state_zipcode(address):
    address=address.split(",")
   
    local_address=address[0]
    city=address[1]
    state_and_zip=address[2].split()
    state=state_and_zip[0]
    zipcode=state_and_zip[1]
   
    address_city_state_zipcode={}


    address_city_state_zipcode["local_address"]=local_address
    address_city_state_zipcode["city"]=city
    address_city_state_zipcode["state"]=state
    address_city_state_zipcode["zipcode"]=zipcode
    return address_city_state_zipcode




# Generate 20 rows of data
data = []
count=0
for i in hospitals:
    hospital_id=next(generate_hospital_id(count+1))
    hospital_name=i
    hospitals_address=hospitals_addresses[hospital_name]
    address=get_city_state_zipcode(hospitals_address)
    source_timestamp_=fake.date_time()
    source_timestamp=str(source_timestamp_)
    create_timestamp_=fake.date_time_between(start_date='-1y')
    create_timestamp=str(create_timestamp_)
    update_timestamp_=create_timestamp_+relativedelta(months=2, days=6)
    update_timestamp=str(update_timestamp_)
    row = {
        "hospital_id": hospital_id,
        "hospital_name": hospital_name,
        "hospital_address": address["local_address"],
        "hospital_district": address["city"],
        "hospital_state": address["state"],
        "hospital_zipcode": address["zipcode"],
        "source_timestamp": source_timestamp,
        "created_timestamp": create_timestamp,
        "updated_timestamp": update_timestamp
    }
    count+=1
    data.append(row)


# schema = {
#     'name': 'avro.example.User',
#     'type': 'record',
#     'fields': [
#         {'name': 'hospital_id', 'type': 'string'},
#         {'name': 'hospital_name', 'type': 'string'},
#         {'name': 'hospital_address', 'type': 'string'},
#         {'name': 'hospital_district', 'type': 'string'},
#         {'name': 'hospital_state', 'type': 'string'},
#         {'name': 'hospital_zipcode', 'type': 'string'},
#         {'name': 'source_timestamp', "type": 'string'},
#         {'name': 'created_timestamp', "type": 'string'},
#         {'name': 'updated_timestamp', "type": 'string'},
#     ]
# }


def upload_to_gcs(bucket_name, dest_blob_name):
    logging.info("****************hospital_data_upload**********************")    
    storage_client=storage_client = storage.Client()
    bucket=storage_client.bucket(bucket_name)
    blob=bucket.blob(dest_blob_name)
    # blob.upload_from_filename("./helpers/hospital_data.csv")
    blob.upload_from_filename("./hospital_data_temp.csv")
    print(blob.public_url)


def write_hos_to_csv(**kwargs):
    # logging.info(kwargs["test_param"])
    with open("./hospital_data_temp.csv", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=Columns)
        writer.writeheader()
        writer.writerows(data)
    time_now=datetime.now().strftime("%Y%m%d%H%M")
    upload_to_gcs("hca_hospital-reports_source_20231005", "hospital_data/hca_hospital_data_full_{}.csv".format(time_now))
    upload_to_gcs("us-central1-hca-datalake-or-34fa5e51-bucket", "data/csvs/hospital_data.csv")
   
if __name__=='__main__':
    pass
    # Write the data to a CSV file


    # schema_parsed = avro.schema.parse(json.dumps(schema))
    # with open('./helpers/hospital_data.avro', 'wb') as f:
    #     writer = DataFileWriter(f, DatumWriter(), schema_parsed)
    #     for row in data:
    #         writer.append(row)
    #     writer.close()


    # upload_to_gcs("us-central1-hca-datalake-or-34fa5e51-bucket","data/csvs/hospital_data.csv")



