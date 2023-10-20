import os, logging
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from helpers.employee_competencies_gen import employee_competencies_gen_start_function


default_args = {
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': '@once',
}


dag = DAG(
    'employee_competencies_gen_dag',
    default_args=default_args,
    description='dag to generate employee competencies data',
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


employee_competencies_generation = PythonOperator(
    task_id='generate_data',
    python_callable=employee_competencies_gen_start_function,
    provide_context=True,
    dag=dag,
)
end_=PythonOperator(
    task_id='end',
    python_callable=end_dag,
    provide_context=True,
    dag=dag,
)


start_>>employee_competencies_generation>>end_


if __name__ == "__main__":
    dag.cli()



