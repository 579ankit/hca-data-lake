import os, logging
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator
from helpers.hospital_data import write_hos_to_csv
from helpers.test_specs import write_tst_to_csv


default_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'hospital_data_and_test_specs_data__batch_load_dag',
    default_args=default_args,
    description='Dag to write hospital_data and test_specs to csv files',
    schedule_interval=None,
)


def start_dag():
    logging.info("Starting the DAG...!")


def end_dag():
    logging.info("DAG Ended....!")


start_ = PythonOperator(
    task_id='start',
    python_callable=start_dag,
    provide_context=True,
    dag=dag,
)


hospital_data_generation = PythonOperator(
    task_id='generate_hospital_data',
    op_kwargs={"test_param": "This is a test param"},
    python_callable=write_hos_to_csv,
    provide_context=True,
    dag=dag,
)


test_specs_data_generation = PythonOperator(
    task_id='generate_test_specs_data',
    python_callable=write_tst_to_csv,
    provide_context=True,
    dag=dag,
)


end_=PythonOperator(
    task_id='end',
    python_callable=end_dag,
    provide_context=True,
    dag=dag,
)




start_>>hospital_data_generation>>test_specs_data_generation>>end_


if __name__ == "__main__":
    dag.cli()



