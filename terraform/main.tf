terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = "gen-lang-client-0097541881"
  region  = "europe-west4"
}

resource "google_storage_bucket" "ecommerce" {
  name     = "ecommerce-data-platform-gen-lang-client-0097541881"
  location = "EUROPE-WEST4"
}

resource "google_bigquery_dataset" "ecommerce" {
  dataset_id = "ecommerce"
  location   = "EUROPE-WEST4"
}