dataflow_job_parameters = [
  {
    input_topic : "projects/hca-data-lake-poc/topics/emp_log_data",
    input_subscription : "projects/hca-data-lake-poc/subscriptions/emp_log_data-sub",
    output_path : "gs://json_to_bq_hca/employee_competencies/"
  }
]
template_gcs_path         = "gs://json_to_bq_hca/dataflow-flex-templates/jsonpubsubtogcs2"
temp_gcs_location         = "gs://json_to_bq_hca/dataflow/temp"
dataflow_staging_location = "gs://json_to_bq_hca/dataflow/staging"
# dataflow_serviceaccount      = "hca-dataflow-datalake-poc-sa@hca-usr-hin-proc-datalake.iam.gserviceaccount.com"
dataflow_project             = "hca-data-lake-poc"
dataflow_region              = "us-central1"
dataflow_vpc_network         = "default"
dataflow_subnetwork_selflink = "regions/us-central1/subnetworks/default"