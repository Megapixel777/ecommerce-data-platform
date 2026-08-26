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

resource "google_bigquery_table" "orders" {
  dataset_id = google_bigquery_dataset.ecommerce.dataset_id
  table_id   = "orders"

  schema = jsonencode([
    {
      name = "order_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "customer_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "event_time"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "status"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "value"
      type = "FLOAT64"
      mode = "NULLABLE"
    }
  ])
}