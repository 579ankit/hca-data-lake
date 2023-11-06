resource "google_dataflow_flex_template_job" "pubsub_to_gcs" {
  for_each                = { for idx, param in var.dataflow_job_parameters : idx => param }
  provider                = google-beta
  project                 = var.dataflow_project
  region                  = var.dataflow_region
  name                    = replace(replace(basename(each.value.input_topic), "_", "-"), "hca", "hca-streaming")
  container_spec_gcs_path = var.template_gcs_path
  staging_location        = var.dataflow_staging_location
  temp_location           = var.temp_gcs_location
  network                 = var.dataflow_vpc_network
  subnetwork              = var.dataflow_subnetwork_selflink
  enable_streaming_engine = true
  ip_configuration        = "WORKER_IP_PRIVATE"
  service_account_email   = var.dataflow_serviceaccount
  on_delete               = "drain"
  parameters = {
    input_subscription = each.value.input_subscription
    output_path        = each.value.output_path
    window_size        = each.value.window_size
    num_shards         = each.value.num_shards
  }
  skip_wait_on_job_termination = false
}