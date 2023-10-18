variable "dataflow_project" {
  description = "GCP project to deploy dataflow job"
  type        = string
}

variable "dataflow_region" {
  description = "GCP Region to deploy dataflow job"
  type        = string
}

variable "template_gcs_path" {
  description = "GCS path of Dataflow Flex template image"
  type        = string
}

variable "temp_gcs_location" {
  description = "Dataflow temporary location"
  type        = string
}

variable "dataflow_staging_location" {
  description = "Dataflow staging location"
  type        = string
}

variable "dataflow_job_parameters" {
  description = "Dataflow Flex template parameters"
  type = list(object({
    input_topic : string,
    input_subscription : string,
    output_path : string
  }))
}

variable "dataflow_vpc_network" {
  description = "Network to be used by dataflow workers"
  type        = string
  # default = "default"

}

variable "dataflow_subnetwork_selflink" {
  description = "Subnetwork self link to be used by dataflow wroker VMs"
  type        = string

}

variable "dataflow_serviceaccount" {
  description = "Dataflow service account"
  type        = string
}