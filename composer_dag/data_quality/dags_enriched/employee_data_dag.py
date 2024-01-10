from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateTaskOperator,
    DataplexDeleteTaskOperator,
)
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubPublishMessageOperator,
)
from helpers.dataplex_reusable_fun import CommonDag
import json

# dataplex constants
CONFIGS_BUCKET_NAME = "hca_dataplex_yaml_storage"
CONFIGS_PATH = f"gs://{CONFIGS_BUCKET_NAME}/enriched_employee_data/employee_data.yaml"
DATAPLEX_TASK_ID = "enriched-employee-data-dag"
DATA_LAKE = 'hr-lake'
# output table data
GCP_PROJECT_ID = "hca-usr-hin-proc-datalake"
GCP_BQ_DATASET_ID = "hca_enriched_employee_data_dq_output"
TARGET_BQ_TABLE = "dag_employee_data_output"

DAG_INTERVAL = '@daily'
PUBSUB_TOPIC = 'dataplex-dag-topic'

common_dag = CommonDag(CONFIGS_PATH, DATAPLEX_TASK_ID, GCP_PROJECT_ID, GCP_BQ_DATASET_ID, TARGET_BQ_TABLE, DATA_LAKE)

def message_encode():
    print('dataset_quality: ', dataset_quality)
    message = dataset_quality
    return json.dumps(message, default=str).encode('utf-8')

def query_message_state():
    return "publish_messages" if dataset_quality else "not_publish"

with models.DAG(
    'enriched_employee_data_dag',
    catchup=False,
    default_args=common_dag.default_args,
    schedule_interval=DAG_INTERVAL,
    user_defined_macros={"pubsub_encode": message_encode},
    render_template_as_native_obj=True,
) as dag:
    print("v1")
    global dataset_quality
    
    start_op = BashOperator(
        task_id="start_task",
        bash_command="echo start",
        dag=dag
    )

    # this will check for the existing dataplex task 
    get_dataplex_task = BranchPythonOperator(
        task_id="get_dataplex_task",
        python_callable= common_dag._get_dataplex_task,
        provide_context=True
    )

    dataplex_task_exists = BashOperator(
        task_id="task_exist",
        bash_command="echo 'Task Already Exists'",
        dag=dag
    )
    dataplex_task_not_exists = BashOperator(
        task_id="task_not_exist",
        bash_command="echo 'Task not Present'",
        dag=dag
    )
    dataplex_task_error = BashOperator(
        task_id="ERROR",
        bash_command="echo 'Error in fetching dataplex task details'",
        dag=dag
    )

    # this will delete the existing dataplex task with the given task_id
    delete_dataplex_task = DataplexDeleteTaskOperator(
        project_id=common_dag.DATAPLEX_PROJECT_ID,
        region=common_dag.DATAPLEX_REGION,
        lake_id=common_dag.DATAPLEX_LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="delete_dataplex_task",
    )

    # this will create a new dataplex task with a given task id
    create_dataplex_task = DataplexCreateTaskOperator(
        project_id=common_dag.DATAPLEX_PROJECT_ID,
        region=common_dag.DATAPLEX_REGION,
        lake_id=common_dag.DATAPLEX_LAKE_ID,
        body=common_dag.EXAMPLE_TASK_BODY,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="create_dataplex_task",
        trigger_rule="none_failed_min_one_success",
    )

    # this will get the status of dataplex task job
    dataplex_task_state = BranchPythonOperator(
        task_id="dataplex_task_state",
        python_callable=common_dag._get_dataplex_job_state,
        provide_context=True,
    )

    dataplex_task_success = BashOperator(
        task_id="SUCCEEDED",
        bash_command="echo 'Job Completed Successfully'",
        dag=dag
    )
    dataplex_task_failed = BashOperator(
        task_id="FAILED",
        bash_command="echo 'Job Failed'",
        dag=dag
    )

    dataset_quality = common_dag._get_query_status()
    message = "{{ pubsub_encode() }}"

    dq_status = BranchPythonOperator(
        task_id="dq_result_status",
        python_callable=query_message_state,
        provide_context=True,
    )

    dq_status_not_publish = BashOperator(
        task_id="not_publish",
        bash_command="echo 'None of the rule failed in DQ check'",
        dag=dag
    )

    publish_task = PubSubPublishMessageOperator(
        task_id="publish_messages",
        project_id=GCP_PROJECT_ID,
        topic=PUBSUB_TOPIC,
        messages=[
            {"data": message}       
        ],
    )

    # this will send logs
    send_logs = BranchPythonOperator(
        task_id="send_logs",
        python_callable=common_dag.send_logs,
        op_args=[dataset_quality],
        provide_context=True
    )
    
start_op >> get_dataplex_task
get_dataplex_task >> [dataplex_task_exists, dataplex_task_not_exists, dataplex_task_error]
dataplex_task_exists >> delete_dataplex_task
delete_dataplex_task >> create_dataplex_task
dataplex_task_not_exists >> create_dataplex_task
create_dataplex_task >> dataplex_task_state
dataplex_task_state >> [dataplex_task_success, dataplex_task_failed]
dataplex_task_success >> dq_status >> [publish_task, dq_status_not_publish]
publish_task >> send_logs
    