terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.47.0"
    }
  }
}

provider "google" {

  project = var.dataflow_project
  region  = var.dataflow_region
}