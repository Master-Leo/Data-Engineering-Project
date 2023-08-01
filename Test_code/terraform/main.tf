
# # ---------------------------------------
# provider "google" {
#   project = var.project
#   region = var.region
#   // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
# }

# # Data Lake Bucket
# # Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
# resource "google_storage_bucket" "data-lake-bucket" {
#   name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
#   location      = var.region

#   # Optional, but recommended settings:
#   storage_class = var.storage_class
#   uniform_bucket_level_access = true

#   versioning {
#     enabled     = true
#   }

#   lifecycle_rule {
#     action {
#       type = "Delete"
#     }
#     condition {
#       age = 30  // days
#     }
#   }

#   force_destroy = true
# }

# # DWH
# # Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
# resource "google_bigquery_dataset" "dataset" {
#   dataset_id = var.BQ_DATASET
#   project    = var.project
#   location   = var.region
# }

#------------------------------------------------------------------------------

provider "google" {
  credentials = file("<PATH_TO_SERVICE_ACCOUNT_JSON>")
  project     = "<YOUR_PROJECT_NAME>"
  region      = "us-central1"
}

resource "google_compute_instance" "vm" {
  name         = "vm-example"
  machine_type = "f1-micro"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable"
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral IP
    }
  }

  metadata_startup_script = "sudo docker run -d -p 80:80 your_docker_image:latest"
  
  service_account {
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

