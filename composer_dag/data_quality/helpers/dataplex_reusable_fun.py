import google.auth
import google.auth.transport.requests
from google.cloud import bigquery
import datetime
import json
import requests
import time
import logging

class CommonDag:
    
    def __init__(self, configs_path, dataplex_task_id, gcp_project_id, gcp_bq_dataset_id, target_bq_table, data_lake):
        self.CONFIGS_PATH = configs_path
        self.DATAPLEX_TASK_ID = dataplex_task_id
        self.GCP_PROJECT_ID = gcp_project_id
        self.GCP_BQ_DATASET_ID = gcp_bq_dataset_id
        self.TARGET_BQ_TABLE = target_bq_table
        self.DATAPLEX_LAKE_ID = data_lake

        self.DATAPLEX_PROJECT_ID = "hca-usr-hin-proc-datalake"
        self.DATAPLEX_REGION = "us-central1"
        self.SERVICE_ACC = "hca-dataplex-datalake-poc--594@hca-usr-hin-proc-datalake.iam.gserviceaccount.com"
        # Public Cloud Storage bucket containing the driver code for executing data quality job. There is one bucket per GCP region.
        self.PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME = "dataplex-clouddq-artifacts" # Public Cloud Storage bucket containing the prebuilt data quality executable artifact and hashsum. There is one bucket per GCP region.
        self.SPARK_FILE_FULL_PATH = f"gs://{self.PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{self.DATAPLEX_REGION}/clouddq_pyspark_driver.py"
        self.CLOUDDQ_EXECUTABLE_FILE_PATH = f"gs://{self.PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{self.DATAPLEX_REGION}/clouddq-executable.zip" # The Cloud Storage path containing the prebuilt data quality executable artifact. There is one bucket per GCP region.
        self.CLOUDDQ_EXECUTABLE_HASHSUM_FILE_PATH = f"gs://{self.PUBLIC_CLOUDDQ_EXECUTABLE_BUCKET_NAME}-{self.DATAPLEX_REGION}/clouddq-executable.zip.hashsum" # The Cloud Storage path containing the prebuilt data quality executable artifact hashsum. There is one bucket per GCP region.
        self.TRIGGER_SPEC_TYPE = "ON_DEMAND"
        self.DATAPLEX_ENDPOINT = "https://dataplex.googleapis.com"
        self.GCP_BQ_REGION = "us-central1"  # GCP BQ region where the data is stored
        self.FULL_TARGET_TABLE_NAME = f"{self.GCP_PROJECT_ID}.{self.GCP_BQ_DATASET_ID}.{self.TARGET_BQ_TABLE}"  # The BigQuery table where the final results of the data quality checks are stored.

        self.PUBSUB_SUB = 'dataplex-dag-topic-sub'

        self.EXAMPLE_TASK_BODY = {
            "spark": {
                "python_script_file": self.SPARK_FILE_FULL_PATH,
                "file_uris": [
                    self.CLOUDDQ_EXECUTABLE_FILE_PATH,
                    self.CLOUDDQ_EXECUTABLE_HASHSUM_FILE_PATH,
                    self.CONFIGS_PATH
                ],
            "infrastructure_spec": {"vpc_network": {"sub_network": f"projects/{self.DATAPLEX_PROJECT_ID}/regions/{self.DATAPLEX_REGION}/subnetworks/hca-datalake-processing-net"}}
            },
            "execution_spec": {
                "service_account": self.SERVICE_ACC,
                "args": {
                    "TASK_ARGS": f"clouddq-executable.zip, \
                        ALL, \
                        {self.CONFIGS_PATH}, \
                        --gcp_project_id={self.GCP_PROJECT_ID}, \
                        --gcp_region_id={self.GCP_BQ_REGION}, \
                        --gcp_bq_dataset_id={self.GCP_BQ_DATASET_ID}, \
                        --target_bigquery_summary_table={self.FULL_TARGET_TABLE_NAME}"
                }
            },
            "trigger_spec": {
                "type_": self.TRIGGER_SPEC_TYPE
            },
            "description": "Clouddq Airflow Task"
        }

        # for best practices
        self.YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

        # default arguments for the dag
        self.default_args = {
            'owner': 'Dataplex DAG',
            'depends_on_past': False,
            'email': [''],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=5),
            'start_date': self.YESTERDAY,
        }


    def get_session_headers(self) -> dict:
        """
        This method is to get the session and headers object for authenticating the api requests using credentials.
        Args:
        Returns: dict
        """
        # getting the credentials and project details for gcp project
        credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

        # getting request object
        auth_req = google.auth.transport.requests.Request()

        credentials.refresh(auth_req)  # refresh token
        auth_token = credentials.token

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + auth_token
        }
        return headers

    def get_clouddq_task_status(self) -> str:
        """
        This method will return the job status for the task.
        Args:
        Returns: str
        """
        headers = self.get_session_headers()
        res = requests.get(
            f"{self.DATAPLEX_ENDPOINT}/v1/projects/{self.DATAPLEX_PROJECT_ID}/locations/{self.DATAPLEX_REGION}/lakes/{self.DATAPLEX_LAKE_ID}/tasks/{self.DATAPLEX_TASK_ID}/jobs",
            headers=headers)
        resp_obj = json.loads(res.text)
        if res.status_code == 200:
            if (
                "jobs" in resp_obj
                and len(resp_obj["jobs"]) > 0
                and "state" in resp_obj["jobs"][0]
            ):
                task_status = resp_obj["jobs"][0]["state"]
                return task_status
        else:
            return "FAILED"

    def _get_dataplex_job_state(self) -> str:
        """
        This method will try to get the status of the job till it is in either 'SUCCEEDED' or 'FAILED' state.
        Args:
        Returns: str
        """
        task_status = self.get_clouddq_task_status()
        while (task_status != 'SUCCEEDED' and task_status != 'FAILED' and task_status != 'CANCELLED' and task_status != 'ABORTED'):
            print(time.ctime())
            time.sleep(30)
            task_status = self.get_clouddq_task_status()
            print(f"CloudDQ task status is {task_status}")
        return task_status

    def _get_dataplex_task(self) -> str:
        """
        This method will return the status for the task.
        Args:
        Returns: str
        """
        headers = self.get_session_headers()
        res = requests.get(
            f"{self.DATAPLEX_ENDPOINT}/v1/projects/{self.DATAPLEX_PROJECT_ID}/locations/{self.DATAPLEX_REGION}/lakes/{self.DATAPLEX_LAKE_ID}/tasks/{self.DATAPLEX_TASK_ID}",
            headers=headers)
        if res.status_code == 404:
            return "task_not_exist"
        elif res.status_code == 200:
            return "task_exist"
        else:
            return "ERROR"

    def _get_query_status(self):
        client = bigquery.Client()

        query0 = f"""
            SELECT * FROM `{self.GCP_PROJECT_ID}.{self.GCP_BQ_DATASET_ID}.__TABLES__`
            """
        response0 = client.query(query0).result()
        views_list = []
        for row in list(response0):
            if row[2].endswith('_output'): 
                views_list.append(row[2])
        
        if self.TARGET_BQ_TABLE in views_list:
            query1 = f"""
                WITH ranked_data AS (
                SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY rule_binding_id, rule_id ORDER BY execution_ts DESC) AS row_number
                FROM `{self.GCP_PROJECT_ID}.{self.GCP_BQ_DATASET_ID}.{self.TARGET_BQ_TABLE}`
                )
                SELECT * FROM ranked_data
                WHERE row_number = 1 and (failed_records_query <> '' or null_count > 0)
                """
            response = client.query(query1).result()
            lst = list(response)
            if len(lst) == 0: return {}

            row_list = []
            table_id = ""
            for row in lst:
                table_id = row[4]
                temp_dict = {
                    "invocation_id": row[0],
                    "execution_ts": row[1],
                    "rule_id": row[3],
                    "column_id": row[5],
                    "rows_validated": row[14],
                    "complex_rule_validation_errors_count": row[15],
                    "complex_rule_validation_success_flag": row[16], 
                    "success_count": row[18],
                    "success_percentage": row[19],
                    "failed_count": row[20],
                    "failed_percentage": row[21],
                    "null_count": row[22],
                    "null_percentage": row[23],
                    "failed_records_query": row[24]
                }
                row_list.append(temp_dict)
            view_dict = {
                "table_id": table_id,
                "dq_output_table_id": f"{self.GCP_PROJECT_ID}.{self.GCP_BQ_DATASET_ID}.{self.TARGET_BQ_TABLE}",
                "data": row_list
            }
            return view_dict

        else: return {}

    def send_logs(self, data_json):
        logging.info(f'Data Quality validation result stored in BigQuery at {self.GCP_BQ_DATASET_ID}.{self.TARGET_BQ_TABLE} and sample error data: {data_json}')
        