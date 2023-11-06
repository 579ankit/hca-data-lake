terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.47.0"
    }
  }
  backend "gcs" {
    bucket = "hca_job_data_20231009"
    prefix = "terraform/statefiles/dataflow"
  }
}

provider "google" {

  project = var.dataflow_project
  region  = var.dataflow_region
}