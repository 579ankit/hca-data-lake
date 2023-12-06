import faker, random, csv, json
from datetime import datetime
from google.cloud import storage


fake = faker.Faker('en-US')


date_=fake.date_time()


test_spec_columns=['test_id', 'source_unit_of_measurement', 'test_mnemonic_standard', 'test_mnemonic_variations', 'test_name', 'test_description', 'test_type', 'standard_unit_of_measurement', 'conversion_factor', 'min_value', 'max_value', 'source_timestamp', 'created_timestamp']
test_spec_data=[]


def generate_test_id(number):
    test_id_prefix = "TST"
    test_id_counter = number
    while True:
        if number%4==0:
            yield ""
        else:
            yield f"{test_id_prefix}-{test_id_counter:04d}"
        # yield f"{test_id_prefix}-{test_id_counter:04d}"
        test_id_counter += 1
count=0


test_names={}
source_test_units={}
test_mnemonic_variations={}
with open("/home/airflow/gcs/data/csvs/lab_test_sample_data.csv", 'r') as file:
    csvreader = csv.reader(file)
    for row in csvreader:
        if row[1]!='test_name' :          
            if row[1] in test_names:
                source_test_units[row[1]].append(row[6])
                source_unit_of_measurement=row[6]
                conversion_factor=row[10]
                # test_mnemonic_variations[row[1]].append(row[2])
            else :
                test_id=next(generate_test_id(count+1))
                count+=1
                test_names[row[1]]=test_id
                source_test_units[row[1]]=[row[6]]
                # test_mnemonic_variations[row[1]]=[i for i in row[2].split("||")]
                test_mnemonic_variations[row[1]]=row[2].replace("||", "|")
                if test_id!="":
                    test_id_num=int(test_id.split("-")[1])
                    if test_id_num%5==0:
                        source_unit_of_measurement=""
                    else:
                        source_unit_of_measurement=row[6]
                # source_unit_of_measurement=row[6]
                test_mnemonic_standard=row[3]
                test_mnemonic_variation=test_mnemonic_variations[row[1]]
                test_name=row[1]
                test_description=row[4]
                test_type=row[5]
                standard_unit_of_measurement=row[7]
                if test_id_num%6==0:
                    conversion_factor=""
                else:
                    conversion_factor=row[10]
                # conversion_factor=row[10]
                min_value=row[8]
                max_value=row[9]
                source_timestamp=fake.date_time_between(start_date='-1y')
                created_timestamp=fake.date_time_between(start_date='-1y')
            row={
                "test_id": test_id,
                "source_unit_of_measurement": source_unit_of_measurement,
                "test_mnemonic_standard": test_mnemonic_standard,
                "test_mnemonic_variations": test_mnemonic_variation,
                "test_name": test_name,
                "test_description": test_description,
                "test_type": test_type,
                "standard_unit_of_measurement": standard_unit_of_measurement,
                "conversion_factor": conversion_factor,
                "min_value": min_value,
                "max_value": max_value,
                "source_timestamp": source_timestamp,
                "created_timestamp": created_timestamp,    }
            test_spec_data.append(row)


def upload_to_gcs(bucket_name, dest_blob_name):
    storage_client=storage_client = storage.Client()
    bucket=storage_client.bucket(bucket_name)
    blob=bucket.blob(dest_blob_name)
    blob.upload_from_filename("./test_specs_temp.csv")
    print(blob.public_url)


def write_tst_to_csv():
    with open("./test_specs_temp.csv", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=test_spec_columns)
        writer.writeheader()
        writer.writerows(test_spec_data)
    time_now=datetime.now().strftime("%Y%m%d%H%M")
    upload_to_gcs("hca_hospital-reports_source_20231005", "test_specs/hca_test_specs_full_{}.csv".format(time_now))
    upload_to_gcs("us-central1-hca-datalake-or-34fa5e51-bucket", "data/csvs/test_specs.csv")



