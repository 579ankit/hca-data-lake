import csv, json, ast
import faker, datetime
import random
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
from google.cloud import storage


from google.cloud.sql.connector import Connector, IPTypes
import pymysql
from sqlalchemy.sql import text
import sqlalchemy
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


from helpers.data_lists import hospital_ids, test_ids, test_details, test_units




# Create a Faker object
fake = faker.Faker()


Columns=["lab_report_id", "patient_id", "test_id", "test_mnemonic","test_timestamp", "test_result_value", "unit_of_mesurement",
         "hospital_id", "test_type", "source_timestamp", "created_timestamp"]




# def generate_patient_id(number):
#     emp_id_prefix = "PAT"
#     emp_id_counter = number
#     while True:
#         yield f"{emp_id_prefix}-{emp_id_counter:04d}"
#         emp_id_counter += 1




# Generate 20 rows of data
data = []
# print(test_ids)
# for i in test_ids:
#     print(i, test_units[i])


def generate_data(patient_ids):
    for i in range(len(patient_ids)):
        lab_report_id=fake.uuid4()
        # patient_id=next(generate_patient_id(i+1))
        patient_id=patient_ids[i]
        test_id=random.choice(test_ids)
        # print(test_id, test_details[test_id])
        # print("*********")
        # ini_list=test_details[test_id][2]
        # mnemonics_lst=ast.literal_eval(ini_list)
        mnemonics_lst=test_details[test_id][2].split("|")
        test_mnemonic1=random.choice(mnemonics_lst)
        test_mnemonic2=random.choice(mnemonics_lst)
        test_mnemonic3=random.choice(mnemonics_lst)
        test_timestamp=test_details[test_id][11]
        test_date1=datetime.datetime.strptime(test_timestamp, "%Y-%m-%d %H:%M:%S")
        test_date2=test_date1+relativedelta(months=2, days=6)
        test_date3=test_date2+relativedelta(months=4, days=4)
        units_list=list(test_units[test_id].items())
        # print(units_list)
        # print("+++++++++++")
        item1=random.choice(units_list)
        unit_of_mesurement1=item1[0]
        cf1=item1[1]
        item2=random.choice(units_list)
        unit_of_mesurement2=item2[0]
        cf2=item2[1]
        item3=random.choice(units_list)
        unit_of_mesurement3=item3[0]
        cf3=item3[1]
        # print(cf1, cf2, cf3)
        test_result1=random.uniform(float(test_details[test_id][8]), float(test_details[test_id][9]))
        if cf1 is int or cf1 is float:
        # if cf1!="No conversion":
            test_result1=round(test_result1/float(cf1), 5)
        test_result2=random.uniform(float(test_details[test_id][8]), float(test_details[test_id][9]))
        if cf2 is int or cf2 is float:
        # if cf2!="No conversion":
            test_result2=round(test_result2/float(cf2), 5)
        test_result3=random.uniform(float(test_details[test_id][8]), float(test_details[test_id][9]))
        if cf3 is int or cf3 is float:
        # if cf3!="No conversion":
            test_result3=round(test_result3/float(cf3), 5)
        hospital_id=random.choice(hospital_ids)
        test_type=test_details[test_id][5]
        source_timestamp=test_details[test_id][10]
        created_timestamp=test_details[test_id][11]
        # print(test_id, unit_of_mesurement1, unit_of_mesurement2, unit_of_mesurement3)
        row1 = {
            "lab_report_id": fake.uuid4(),
            "patient_id": patient_id,
            "test_id": test_id,
            "test_mnemonic": test_mnemonic1,
            "test_timestamp": test_date1,
            "test_result_value": test_result1,
            "unit_of_mesurement": unit_of_mesurement1,
            "hospital_id": hospital_id,
            "test_type": test_type,
            "source_timestamp": source_timestamp,
            "created_timestamp": created_timestamp,
        }
        # row2 = {
        #     "lab_report_id": fake.uuid4(),
        #     "patient_id": patient_id,
        #     "test_id": test_id,
        #     "test_mnemonic": test_mnemonic2,
        #     "test_timestamp": test_date2,
        #     "test_result_value": test_result2,
        #     "unit_of_mesurement": unit_of_mesurement2,
        #     "hospital_id": hospital_id,
        #     "test_type": test_type,
        #     "source_timestamp": source_timestamp,
        #     "created_timestamp": created_timestamp,
        #     # "log_timestamp": fake.date_time(),
        # }
        # row3 = {
        #     "lab_report_id": fake.uuid4(),
        #     "patient_id": patient_id,
        #     "test_id": test_id,
        #     "test_mnemonic": test_mnemonic3,
        #     "test_timestamp": test_date3,
        #     "test_result_value": test_result3,
        #     "unit_of_mesurement": unit_of_mesurement3,
        #     "hospital_id": hospital_id,
        #     "test_type": test_type,
        #     "source_timestamp": source_timestamp,
        #     "created_timestamp": created_timestamp,
        #     # "log_timestamp": fake.date_time(),
        # }
        data.append(row1)
        # data.append(row2)
        # data.append(row3)


instance_connection_name=Variable.get("instance_connection_name")
db_user = Variable.get("DB_USER")  # e.g. 'my-db-user'
db_pass = Variable.get("DB_PASS")  # e.g. 'my-db-password'
db_name = Variable.get("DB_NAME")  # e.g. 'my-database'
table_name=Variable.get("table_name")


ip_type = IPTypes.PUBLIC


connector = Connector(ip_type)


def getconn() -> pymysql.connections.Connection:
    conn: pymysql.connections.Connection = connector.connect(
        instance_connection_name,
        "pymysql",
        user=db_user,
        password=db_pass,
        db=db_name
    )
    return conn


def get_random_patients():
    pool=sqlalchemy.create_engine("mysql+mysqlconnector://root:hcaroot@34.28.159.129/hca_patient_db_raw", future=True)
    with pool.connect() as db_conn:
        result2 = db_conn.execute(sqlalchemy.text("select patient_id from {0} order by  RAND() limit 100;".format(table_name))).fetchall()
        patient_ids=[]
        for i in result2:
            patient_ids.append(i[0])
        generate_data(patient_ids)
        db_conn.commit()


def upload_csv_to_gcs(bucket_name, dest_blob_name):
    storage_client = storage.Client()
    bucket=storage_client.bucket(bucket_name)
    blob=bucket.blob(dest_blob_name)
    blob.upload_from_filename("./lab_report_data_temp.csv")
    print(blob.public_url)


json_dest_bucket=""
json_dest_blob=""


def upload_json_to_gcs(bucket_name, dest_blob_name):
    storage_client= storage.Client()
    json_dest_bucket=bucket_name
    json_dest_blob=dest_blob_name
    bucket=storage_client.bucket(bucket_name)
    blob=bucket.blob(dest_blob_name)
    blob.upload_from_filename("./lab_report_data_temp.json")
    print(blob.public_url)


# def write_lrd_to_json():
#     data_json_obj=json.dumps(data, default=str)
#     with open("./lab_report_data_temp.json", "w") as outfile:
#         outfile.write(data_json_obj)
#     time_now=dt.now().strftime("%Y%m%d%H%M")
#     upload_json_to_gcs("us-central1-hca-datalake-or-34fa5e51-bucket", "data/json/lab_report_data_{}.json".format(time_now))




def convert_csv_to_json(csvFilePath):
    data = []
    # Open a csv reader called DictReader
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for rows in csvReader:
            dict_={}
            dict_['lab_report_id']= rows['lab_report_id']
            dict_['patient_id']= rows['patient_id']
            dict_['test_id']= rows['test_id']
            dict_['test_mnemonic']= rows['test_mnemonic']
            dict_['test_timestamp']= rows['test_timestamp']
            dict_['test_result_value']= rows['test_result_value']
            dict_['unit_of_mesurement']= rows['unit_of_mesurement']
            dict_['hospital_id']= rows['hospital_id']
            dict_['test_type']= rows['test_type']
            dict_['source_timestamp']= rows['source_timestamp']
            dict_['created_timestamp']= rows['created_timestamp']            


            data.append(dict_)
    # Open a json writer, and use the json.dumps()
    # function to dump data
    with open("./lab_report_data_temp.json", 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=2))
    time_now=dt.now().strftime("%Y%m%d%H%M")
    upload_json_to_gcs("test_bucket_09122023", "lab_report_data/json/lab_report_data_{}.json".format(time_now))


def write_lrd_to_csv():
    get_random_patients()
    with open("./lab_report_data_temp.csv", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=Columns)
        writer.writeheader()
        writer.writerows(data)
    convert_csv_to_json("./lab_report_data_temp.csv")
    time_now=dt.now().strftime("%Y%m%d%H%M")
    upload_csv_to_gcs("test_bucket_09122023", "lab_report_data/csvs/lab_report_data_{}.csv".format(time_now))




if __name__=='__main__':
    pass
    # # Write the data to a CSV file
    # with open("./data/lab_report_data.csv", "w", newline='') as f:
    #     writer = csv.DictWriter(f, fieldnames=Columns)
    #     writer.writeheader()
    #     writer.writerows(data)


    # # Write the datat to json    
    # data_json_obj=json.dumps(data, default=str)
    # # print(data_json_obj)
    # with open("./data/lab_report_data.json", "w") as outfile:
    #     outfile.write(data_json_obj)



