terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# GCS Buckets
resource "google_storage_bucket" "bronze" {
  name          = "turtletide-bronze-${var.project_id}"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "silver" {
  name          = "turtletide-silver-${var.project_id}"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "gold" {
  name          = "turtletide-gold-${var.project_id}"
  location      = var.region
  force_destroy = true
}

# BigQuery Dataset
resource "google_bigquery_dataset" "gold" {
  dataset_id = "turtletide_gold"
  location   = var.region
}