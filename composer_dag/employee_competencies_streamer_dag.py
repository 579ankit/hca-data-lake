import os, logging
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from helpers.employee_competencies_streamer import employee_competencies_streamer_start_function

default_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'employee_competencies_streamer_dag',
    default_args=default_args,
    description='dag to stream employee competencies to a pubsub topic',
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

employee_competencies_streamer = PythonOperator(
    task_id='stream_data',
    python_callable=employee_competencies_streamer_start_function,
    provide_context=True,
    dag=dag,
)
end_=PythonOperator(
    task_id='end',
    python_callable=end_dag,
    provide_context=True,
    dag=dag,
)

start_>>employee_competencies_streamer>>end_

if __name__ == "__main__":
    dag.cli()
