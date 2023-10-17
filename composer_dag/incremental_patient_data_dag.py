import os, logging
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from helpers.patient_data_update import  insert_update_to_cloud_sql


default_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'synthatic_data_genaration_cloud_sql_final7',
    default_args=default_args,
    description='dag to update and insert data into cloud sql',
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


data_generation = PythonOperator(
    task_id='generate_data',
    python_callable=insert_update_to_cloud_sql,
    provide_context=True,
    dag=dag,
)
end_=PythonOperator(
    task_id='end',
    python_callable=end_dag,
    provide_context=True,
    dag=dag,
)




start_>>data_generation>>end_


if __name__ == "__main__":
    dag.cli()



