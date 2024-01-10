dataflow_job_parameters = [
  {
    input_topic : "projects/hca-usr-hin-datalake-poc/topics/hca_employee_competencies_json",
    input_subscription : "projects/hca-usr-hin-datalake-poc/subscriptions/hca_employee_competencies_json-sub",
    output_path : "gs://hca_employee-data_landing_20231005/employee_competencies_json/",
    window_size : 10,
    num_shards : 1

  },
  {
    input_topic : "projects/hca-usr-hin-datalake-poc/topics/hca_lab_report_data_json",
    input_subscription : "projects/hca-usr-hin-datalake-poc/subscriptions/hca_lab_report_data_json-sub",
    output_path : "gs://hca_hospital-reports_landing_20231005/lab_report_data_json/"
  }
]
template_gcs_path            = "gs://hca_job_data_20231009/dataflow/dataflow-flex-templates/Json_pubsub_to_gcs"
temp_gcs_location            = "gs://hca_job_data_20231009/dataflow/temp"
dataflow_staging_location    = "gs://hca_job_data_20231009/dataflow/staging"
dataflow_serviceaccount      = "hca-dataflow-datalake-poc-sa@hca-usr-hin-proc-datalake.iam.gserviceaccount.com"
dataflow_project             = "hca-usr-hin-proc-datalake"
dataflow_region              = "us-central1"
dataflow_vpc_network         = "hca-datalake-processing-net"
dataflow_subnetwork_selflink = "regions/us-central1/subnetworks/hca-datalake-processing-net"