terraform {
  required_version = ">= 0.13"
  required_providers {

    google = {
      source  = "hashicorp/google"
      version = ">= 3.53, < 5.0"
    }
  }
  backend "gcs" {
    bucket = "hca_job_data_20231009"
    prefix = "terraform/statefiles/datastream"
  }
}