terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.47.0"
    }
  }
  backend "gcs" {
    bucket = "json_to_bq_hca"
    prefix = "terraform/state"
  }
}

provider "google" {

  project = var.dataflow_project
  region  = var.dataflow_region
}