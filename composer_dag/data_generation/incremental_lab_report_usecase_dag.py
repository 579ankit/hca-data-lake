import os, logging
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from helpers.lab_report_data import write_lrd_to_csv
from helpers.stream_to_pubsub import start_stream
from helpers.lab_report_object import get_lab_test_details


default_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'Write_lab_report_data_dag_and_stream_upload_objects',
    default_args=default_args,
    description='Dag to write lab_report_data to csv and json format and stream it to pubsub and upload lab report objects',
    schedule_interval="*/5 * * * *",
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


lab_report_data_generation = PythonOperator(
    task_id='generate_lab_report_data_csv_and_json',
    python_callable=write_lrd_to_csv,
    provide_context=True,
    dag=dag,
)




start_pubsub_stream = PythonOperator(
    task_id='streaming_to_pubsub',
    python_callable=start_stream,
    provide_context=True,
    dag=dag,
)


upload_lab_report_objects = PythonOperator(
    task_id='uploading_lab_report_pngs',
    python_callable=get_lab_test_details,
    provide_context=True,
    dag=dag,
)


end_=PythonOperator(
    task_id='end',
    python_callable=end_dag,
    provide_context=True,
    dag=dag,
)




start_>>lab_report_data_generation>>start_pubsub_stream>>upload_lab_report_objects>>end_


if __name__ == "__main__":
    dag.cli()





