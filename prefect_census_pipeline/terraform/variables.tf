 variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "data_bucket_name" {
  description = "Name of the GCS bucket for storing data"
  type        = string
}

variable "bucket_location" {
  description = "Location for the GCS bucket"
  type        = string
}

variable "dataset_id" {
  description = "ID of the BigQuery dataset"
  type        = string
}

variable "dataset_location" {
  description = "Location for the BigQuery dataset"
  type        = string
}

variable "scheduler_job_name" {
  description = "Name of the Cloud Scheduler job for triggering Airflow DAG"
  type        = string
}

variable "airflow_webserver_url" {
  description = "URL of the Airflow webserver"
  type        = string
}

variable "airflow_api_token" {
  description = "API token for authenticating with the Airflow API"
  type        = string
}
# ------------------------------------------------------------------

locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6" 
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}
