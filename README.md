# E-Commerce Data Platform

End-to-end data engineering platform for an e-commerce business, built with Python and PySpark.

The project combines local data engineering practices with Google Cloud infrastructure, Infrastructure as Code, containerization, automated testing, and CI/CD.

## Overview

This project implements an end-to-end data pipeline based on the Olist e-commerce dataset.

The local pipeline ingests raw CSV data, applies data quality and transformation rules, generates business-level aggregations, and stores the resulting datasets in Parquet format following a Medallion Architecture.

The project also includes a Google Cloud pipeline using Google Cloud Storage, Managed Service for Apache Spark, and BigQuery.

Infrastructure is managed using Terraform, while GitHub Actions automates code quality checks, testing, Docker builds, and Terraform validation.

## Architecture

The project currently contains two related processing flows:

### Local Medallion Pipeline

                    Olist CSV

                       │

                       ▼

                ┌──────────────┐
                │    Bronze    │
                │ Raw ingestion│
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐
                │    Silver    │
                │ Cleaned data │
                │ Validations  │
                └──────┬───────┘
                       │
                       ▼
                ┌──────────────┐
                │     Gold     │
                │   Business   │
                │  aggregations│
                └──────────────┘

### Google Cloud Pipeline

                         GCS
                          │
                          │ raw/orders/*.json
                          ▼
                Managed Service for
                  Apache Spark
                          │
                          ▼
                     PySpark
                          │
                          ▼
                   Transformation
                          │
                          ▼
                      BigQuery
                          │
                          ▼
                  orders_summary

### Infrastructure and CI/CD

                         GitHub
                           │
                           ▼
                    GitHub Actions
                           │
              ┌────────────┼────────────┐
              │            │            │
           Quality       Docker     Terraform
              │            │            │
              └────────────┼────────────┘
                           │
                           ▼
                          GCP
                    ┌──────┴──────┐
                    │             │
                   GCS        BigQuery

## Medallion Architecture

### Bronze

Raw Olist data is loaded using explicit PySpark schemas.

Current ingestion components include:

- Orders
- Customers
- Order items

A generic CSV loader is used to avoid duplicating ingestion logic across datasets.

### Silver

The order_items dataset is cleaned and transformed.

Current transformations include:

- Filtering null prices
- Filtering negative prices
- Filtering null freight values
- Filtering negative freight values
- Calculating total_item_value
- Extracting shipping_year
- Extracting shipping_month

Silver data is stored in Parquet and partitioned by:

- shipping_year
- shipping_month

### Gold

Business-level aggregations are generated from the Silver layer.

**Order Sales**

The order_sales dataset contains one row per order with:

- total_order_value
- item_count

**KPIs**

The final KPI dataset contains:

- total_orders
- total_revenue
- average_order_value
- total_items

## Google Cloud Platform

The project also includes a GCP-based processing pipeline.

### Google Cloud Storage

Google Cloud Storage is used as the cloud storage layer.

The current test pipeline reads order JSON files from:

gs://ecommerce-data-platform-gen-lang-client-0097541881/raw/orders/

The project also stores the Spark processing script in:

gs://ecommerce-data-platform-gen-lang-client-0097541881/scripts/

### Managed Service for Apache Spark

The project uses Managed Service for Apache Spark to execute PySpark batch jobs without maintaining a permanent Dataproc cluster.

The current Spark job:

- Reads JSON order data from GCS.
- Infers the input schema.
- Displays the input data.
- Groups orders by status.
- Calculates order count.
- Calculates total revenue.
- Calculates average order value.
- Writes the aggregated result to BigQuery.

The current test execution produces:

| status    | orders_count | total_revenue | average_order_value |
|-----------|---------------|----------------|----------------------|
| delivered | 8             | 575.0          | 71.875               |

### BigQuery

BigQuery is used as the analytical warehouse.

The current dataset is:

ecommerce

The current tables include:

- orders
- orders_summary

The orders_summary table contains:

- status
- orders_count
- total_revenue
- average_order_value

The Spark pipeline writes the aggregation directly to:

ecommerce.orders_summary

## Infrastructure as Code

Terraform is used to manage the GCP infrastructure.

Current Terraform-managed resources include:

- Google Cloud Storage bucket
- BigQuery dataset
- BigQuery orders table
- BigQuery orders_summary table

The Terraform configuration is located in:

terraform/
├── main.tf
└── .terraform.lock.hcl

Terraform is also used to validate that the declared infrastructure matches the actual infrastructure in GCP.

The expected final Terraform state is:

Terraform
│
├── GCS bucket
├── BigQuery dataset
├── orders
└── orders_summary

The Dataproc cluster resource was intentionally not kept in the final infrastructure because the project uses Managed Service for Apache Spark for batch execution instead of maintaining a permanent cluster.

## Project Structure

ecommerce-data-platform/

│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── config/
│
├── data/
│
├── docker/
│
├── docs/
│
├── notebooks/
│
├── scripts/
│
├── sql/
│
├── src/
│   ├── ingestion/
│   │   ├── loader.py
│   │   ├── olist_bronze.py
│   │   └── schemas.py
│   │
│   ├── silver/
│   │   └── order_items.py
│   │
│   ├── gold/
│   │   ├── order_sales.py
│   │   └── kpis.py
│   │
│   ├── streaming/
│   │   └── dataproc_test.py
│   │
│   ├── utils/
│   │   └── storage.py
│   │
│   ├── spark.py
│   └── pipeline.py
│
├── terraform/
│   ├── main.tf
│   └── .terraform.lock.hcl
│
├── tests/
│
├── pyproject.toml
├── docker-compose.yml
├── dockerfile
├── .gitignore
└── README.md

## Tech Stack

- Python 3.11
- PySpark 4.0.1
- Delta Lake 4.0.1
- Parquet
- Pytest
- Ruff
- Docker
- Git
- GitHub
- GitHub Actions
- Google Cloud Storage
- BigQuery
- Managed Service for Apache Spark
- Terraform

## Installation

The project requires Python 3.11.

Install the project together with its development dependencies:

pip install -e ".[dev]"

The editable installation allows the local source code to be used directly by the installed project.

## Configuration

Integration tests require the location of the Olist dataset to be provided through the `OLIST_DATA_PATH` environment variable.

The project uses python-dotenv to load local environment variables.

Example:

OLIST_DATA_PATH=C:\path\to\olist

The expected dataset directory contains the Olist CSV files used by the integration tests.

Environment files containing secrets or local configuration should not be committed to Git.

## Running Tests

Run the unit and non-integration tests:

pytest -m "not integration"

Run the integration tests:

pytest -m integration

Run the complete test suite:

pytest

## Code Quality

Ruff is used for linting, import organization, and Python code quality checks.

Run Ruff with:

ruff check .

Format Terraform files with:

terraform -chdir=terraform fmt

Validate Terraform configuration with:

terraform -chdir=terraform validate

Review the Terraform execution plan with:

terraform -chdir=terraform plan

## Docker

The project includes Docker configuration for reproducible local execution.

Build the Docker image with:

docker compose build

Docker image building is also automatically tested through GitHub Actions.

## CI/CD

GitHub Actions runs automatically on pushes and pull requests targeting the main branch.

The current CI pipeline contains three jobs:

Git push / Pull Request
          │
          ▼
    GitHub Actions
          │
     ┌────┼────────────┐
     │    │            │
     ▼    ▼            ▼
 Quality Docker     Terraform
     │    │            │
     │    │            ├── Terraform format
     │    │            ├── Terraform init
     │    │            └── Terraform validate
     │    │
     │    └── Docker build
     │
     ├── Ruff
     └── Pytest

The Docker job depends on the successful completion of the quality job.

Terraform validation is performed independently.

Integration tests are kept separate because they require the local Olist dataset.

## Data Quality

The project includes automated data quality checks for the Orders dataset.

Current checks include:

- Null order IDs
- Null customer IDs
- Duplicate order IDs
- Orphan customer IDs

These checks are covered by automated tests.

## Storage

The local pipeline writes datasets in Parquet format.

The storage utility supports optional partitioning:

    write_parquet(
        df,
        output_path,
        partition_by=["shipping_year", "shipping_month"],
    )

The Silver order_items dataset is partitioned by:

- shipping_year
- shipping_month

This produces a structure similar to:

silver/order_items/

├── shipping_year=2017/
│   ├── shipping_month=1/
│   ├── shipping_month=2/
│   └── ...
│
└── shipping_year=2018/
    ├── shipping_month=1/
    └── ...

## Local Pipeline

The local pipeline processes Olist order items through the Bronze, Silver, and Gold layers:

olist_order_items_dataset.csv

            │
            ▼

      load_order_items()

            │
            ▼

         Bronze

            │
            ▼

   transform_order_items()

            │
            ▼

         Silver

            │
            ▼

    build_order_sales()

            │
            ▼

     Gold / order_sales

            │
            ▼

        build_kpis()

            │
            ▼

       Gold / KPIs

The pipeline writes each layer to Parquet and returns the final KPI DataFrame.

## GCP Pipeline

The current cloud test pipeline processes JSON order data using Managed Service for Apache Spark:

GCS
raw/orders/*.json
        │
        ▼
Managed Service for Apache Spark
        │
        ▼
dataproc_test.py
        │
        ├── Read JSON
        │
        ├── Schema inspection
        │
        ├── Group by status
        │
        ├── Count orders
        │
        ├── Calculate revenue
        │
        └── Calculate average order value
        │
        ▼
BigQuery
ecommerce.orders_summary

The job can be submitted with:

    gcloud dataproc batches submit pyspark \
      gs://ecommerce-data-platform-gen-lang-client-0097541881/scripts/dataproc_test.py \
      --region=europe-west4 \
      --version=2.2

## Testing Strategy

The project uses Pytest for automated testing.

Tests are divided into:

### Unit Tests

Unit tests validate individual transformations and business rules without relying on external datasets.

Examples include:

- Orders data quality
- Silver order item transformations
- Gold order sales aggregation
- Gold KPI calculations
- Parquet storage

### Integration Tests

Integration tests execute the real ingestion and pipeline against the Olist dataset.

They are marked with:

@pytest.mark.integration

and can be executed with:

pytest -m integration

## Terraform Workflow

Terraform is used to keep the cloud infrastructure reproducible.

Typical workflow:

terraform -chdir=terraform fmt
terraform -chdir=terraform validate
terraform -chdir=terraform plan

When an existing GCP resource needs to be brought under Terraform management, it can be imported into the Terraform state.

After importing a resource, the expected result is:

No changes. Your infrastructure matches the configuration.

This confirms that the Terraform configuration and the actual GCP infrastructure are aligned.

## Current Status

### Completed

- [x] Project structure
- [x] PySpark ingestion
- [x] Explicit data schemas
- [x] Generic CSV loader
- [x] Bronze ingestion
- [x] Silver transformations
- [x] Gold aggregations
- [x] Parquet storage
- [x] Parquet partitioning
- [x] Data quality tests
- [x] Unit tests
- [x] Integration tests
- [x] Ruff linting
- [x] GitHub Actions CI
- [x] Automated test execution in CI
- [x] Docker configuration
- [x] Docker build in CI
- [x] Google Cloud Storage integration
- [x] BigQuery dataset
- [x] BigQuery orders table
- [x] BigQuery orders_summary table
- [x] Managed Service for Apache Spark batch processing
- [x] GCS → Spark → BigQuery pipeline
- [x] Terraform infrastructure
- [x] Terraform state management
- [x] Terraform validation in CI

### Planned

- [ ] Incremental processing
- [ ] Data orchestration
- [ ] Production configuration
- [ ] Monitoring and observability
- [ ] Advanced IAM and service account configuration
- [ ] Databricks deployment
- [ ] Spark SQL transformations
- [ ] Power BI semantic model
- [ ] Production-ready deployment pipeline
- [ ] Additional streaming architecture on GCP

## Project Status

🚧 Project under development.

The current implementation demonstrates an end-to-end data engineering workflow combining local PySpark processing with cloud-based processing and analytics on Google Cloud.