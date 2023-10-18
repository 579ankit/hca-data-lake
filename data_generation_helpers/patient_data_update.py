import os
import csv, logging
import faker
import random


from google.cloud.sql.connector import Connector, IPTypes
import pymysql
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.sql import text
import sqlalchemy


from datetime import datetime
from helpers.data_lists import Job_Class_Code_and_Desc, department_names, hospital_ids, hospital_details, hospital_job_types, hospital_job_titles, employment_statuses, majors
from dateutil.relativedelta import relativedelta


# Create a Faker object
fake = faker.Faker()


Columns=["patient_id", "first_name", "last_name", "date_of_birth", "gender", "address", "hospital_id",
         "phone_number", "email_address", "emergency_contact", "source_timestamp", "created_timestamp", "updated_timestamp"]




def generate_patient_id(number):
    emp_id_prefix = "PAT"
    emp_id_counter = number
    while True:
        yield f"{emp_id_prefix}-{emp_id_counter:04d}"
        emp_id_counter += 1


def gen_phone():
    first = str(random.randint(100,999))
    second = str(random.randint(1,888)).zfill(3)


    last = (str(random.randint(1,9998)).zfill(4))
    while last in ['1111','2222','3333','4444','5555','6666','7777','8888']:
        last = (str(random.randint(1,9998)).zfill(4))
       
    return '{}-{}-{}'.format(first,second, last)


# Generate 10 rows of data
data = []
def generate_data(count):
    logging.info("Inside generate_data function....!")
    for i in range(10):
        patient_id=next(generate_patient_id(count+1))
        count+=1
        gender=random.choice(["Male", "Female"])
        if gender=="Male":
            first_name=fake.first_name_male()
        else:
            first_name=fake.first_name_female()
        last_name=fake.last_name()
        date_of_birth=fake.date_of_birth(minimum_age=25, maximum_age=65)
        address=fake.address()
        hospital_id=random.choice(hospital_ids)
        phone_number=gen_phone()
        email_address="{0}.{1}.{2}@gmail.com".format(first_name, last_name, i)
        emergency_contact=gen_phone()
        source_timestamp=fake.date_between(start_date='-1y')
        create_timestamp=fake.date_between(start_date='-1y')
        update_timestamp=create_timestamp+relativedelta(months=2, days=6)
        row = {
            "patient_id": patient_id,
            "first_name": first_name,
            "last_name": last_name,
            "date_of_birth": date_of_birth,
            "gender": gender,
            "address": address,
            "hospital_id": hospital_id,
            "phone_number": phone_number,
            "email_address": email_address,
            "emergency_contact": emergency_contact,
            "source_timestamp": source_timestamp,
            "created_timestamp": create_timestamp,
            "updated_timestamp": update_timestamp,
        }
        data.append(row)
    logging.info("Returning from generate_data function....!")
    return data




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


def insert_update_to_cloud_sql():
    logging.info("insert_update_to_cloud_sql started......!")
    # pool = sqlalchemy.create_engine("mysql+pymysql://",creator=getconn, future=True)
    pool=sqlalchemy.create_engine("mysql+mysqlconnector://root:hcaroot@34.28.159.129/hca_patient_db_raw", future=True)
    with pool.connect() as db_conn:
        # insert into database
        logging.info("Inside with clause.....!")
        # query database
        result = db_conn.execute(sqlalchemy.text("SELECT count(*) from {0}".format(table_name))).fetchall()


        row_count=result[0][0]
        logging.info("calling generate_data function....!")        
        data=generate_data(row_count)
        logging.info("Inserting rows to SQL....!")
        # Inserting rows into mysql table
        for i in data:
            mydict = i
            columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in mydict.keys())
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
            insert_stmnt = text("INSERT INTO %s ( %s ) VALUES ( %s );" % (table_name, columns, values))
            db_conn.execute(insert_stmnt)


        # Updating random rows    
        logging.info("Counting SQL rows....!")
        result2 = db_conn.execute(sqlalchemy.text("select patient_id from {0} order by  RAND() limit 10;".format(table_name))).fetchall()
        patient_ids=[]
        for i in result2:
            patient_ids.append(i[0])
        logging.info("Updating sql rows....!")
        for pat_id in patient_ids:
            update_stmnt="UPDATE {0} SET last_name ='{1}' where patient_id='{2}'".format(table_name, fake.last_name(), pat_id)
            db_conn.execute(sqlalchemy.text(update_stmnt))
       
        rows = db_conn.execute(sqlalchemy.text("SELECT count(*) from {0}".format(table_name))).fetchall()
        db_conn.commit()
        row_count=rows[0][0]        
        logging.info("insert_update_to_cloud_sql ended......!")


if __name__ == "__main__":
    insert_update_to_cloud_sql()



