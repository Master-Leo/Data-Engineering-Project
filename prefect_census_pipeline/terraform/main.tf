
# Define the provider (GCP in this example)
provider "google" {
  credentials = file("<path_to_service_account_key_file>")
  project     = "<project_id>"
  region      = "<region>"
}

# Define the GCS bucket for storing data
resource "google_storage_bucket" "data_bucket" {
  name     = "<data_bucket_name>"
  location = "<bucket_location>"
}

# Define the GCP BigQuery dataset
resource "google_bigquery_dataset" "data_dataset" {
  dataset_id = "<dataset_id>"
  location   = "<dataset_location>"
}

# Define the Airflow environment variables
resource "google_cloud_scheduler_job" "airflow_scheduler" {
  name        = "<scheduler_job_name>"
  description = "Airflow Scheduler"
  schedule    = "0 0 * * *"
  time_zone   = "UTC"
  http_target {
    http_method = "POST"
    uri         = "https://<airflow_webserver_url>/api/experimental/dags/census_pipeline/dag_runs"
    headers = {
      "Authorization" = "Bearer <airflow_api_token>"
      "Content-Type"  = "application/json"
    }
    body         = <<EOF
{
  "conf": "{}"
}
EOF
  }
}

# Output the GCS bucket and BigQuery dataset information
output "bucket_name" {
  value = google_storage_bucket.data_bucket.name
}

output "dataset_id" {
  value = google_bigquery_dataset.data_dataset.dataset_id
}


terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}
# ---------------------------------------
provider "google" {
  project = var.project
  region = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}
