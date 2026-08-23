# E-Commerce Data Platform

End-to-end data engineering platform for an e-commerce business, built with Python and PySpark using a Medallion Architecture.

## Overview

This project implements an end-to-end data pipeline based on the Olist e-commerce dataset.

The pipeline ingests raw CSV data, applies data quality and transformation rules, generates business-level aggregations, and stores the resulting datasets in Parquet format.

The project also includes automated testing, code quality checks, and CI/CD using GitHub Actions.

## Architecture

The platform follows a Medallion Architecture:

```text
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
                │ Business     │
                │ aggregations │
                └──────────────┘
```

### Bronze

Raw Olist data is loaded using explicit PySpark schemas.

Current ingestion components include:

- Orders
- Customers
- Order items

A generic CSV loader is used to avoid duplicating ingestion logic across datasets.

### Silver

The `order_items` dataset is cleaned and transformed.

Current transformations include:

- Filtering null prices
- Filtering negative prices
- Filtering null freight values
- Filtering negative freight values
- Calculating `total_item_value`
- Extracting `shipping_year`
- Extracting `shipping_month`

Silver data is stored in Parquet and partitioned by:

- `shipping_year`
- `shipping_month`

### Gold

Business-level aggregations are generated from the Silver layer.

#### Order Sales

The `order_sales` dataset contains one row per order with:

- `total_order_value`
- `item_count`

#### KPIs

The final KPI dataset contains:

- `total_orders`
- `total_revenue`
- `average_order_value`
- `total_items`

## Project Structure

```text
ecommerce-data-platform/
│
├── .github/
│   └── workflows/
│       └── ci.yml
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
│   ├── utils/
│   │   └── storage.py
│   │
│   └── pipeline.py
│
├── tests/
│   ├── test_orders_quality.py
│   ├── test_olist_bronze.py
│   ├── test_customers_bronze.py
│   ├── test_order_items_silver.py
│   ├── test_order_sales_gold.py
│   ├── test_kpis_gold.py
│   ├── test_storage.py
│   └── test_pipeline.py
│
├── pyproject.toml
├── .gitignore
└── README.md
```

## Tech Stack

- Python 3.12
- PySpark 4.x
- Pytest
- Ruff
- Parquet
- Git
- GitHub
- GitHub Actions
- Databricks (planned)
- Delta Lake (planned)
- Spark SQL (planned)
- Power BI (planned)

## Installation

Install the project together with its development dependencies:

```bash
pip install -e ".[dev]"
```

The editable installation allows the local source code to be used directly by the installed project.

## Configuration

Integration tests require the location of the Olist dataset to be provided through the `OLIST_DATA_PATH` environment variable.

The project uses `python-dotenv` to load local environment variables.

Example:

```text
OLIST_DATA_PATH=C:\path\to\olist
```

The expected dataset directory contains the Olist CSV files used by the integration tests.

## Running Tests

Run the unit and non-integration tests:

```bash
pytest -m "not integration"
```

Run the integration tests:

```bash
pytest -m integration
```

Run the complete test suite:

```bash
pytest
```

## Code Quality

Ruff is used for linting, import organization, and Python code quality checks.

Run Ruff with:

```bash
ruff check .
```

## CI/CD

GitHub Actions runs automatically on pushes and pull requests targeting the main branch.

The CI pipeline:

1. Checks out the repository.
2. Sets up Python 3.12.
3. Installs the project and development dependencies.
4. Runs Ruff.
5. Runs the non-integration test suite.

```text
Git push / Pull Request
          │
          ▼
    GitHub Actions
          │
          ├── Install dependencies
          │
          ├── Ruff
          │
          └── Pytest
                │
                ▼
             Success
```

Integration tests are kept separate because they require the local Olist dataset.

## Data Quality

The project includes automated data quality checks for the Orders dataset.

Current checks include:

- Null order IDs
- Null customer IDs
- Duplicate order IDs
- Orphan customer IDs

## Storage

The pipeline writes datasets in Parquet format.

The storage utility supports optional partitioning:

```python
write_parquet(
    df,
    output_path,
    partition_by=["shipping_year", "shipping_month"],
)
```

The Silver `order_items` dataset is partitioned by:

- `shipping_year`
- `shipping_month`

This produces a structure similar to:

```text
silver/order_items/
├── shipping_year=2017/
│   ├── shipping_month=1/
│   ├── shipping_month=2/
│   └── ...
└── shipping_year=2018/
    ├── shipping_month=1/
    └── ...
```

## Pipeline

The current pipeline processes Olist order items through the Bronze, Silver, and Gold layers:

```text
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
```

The pipeline writes each layer to Parquet and returns the final KPI DataFrame.

## Testing Strategy

The project uses Pytest for automated testing.

Tests are divided into:

### Unit tests

Test individual transformations and business rules without relying on external datasets.

Examples include:

- Orders data quality
- Silver order item transformations
- Gold order sales aggregation
- Gold KPI calculations
- Parquet storage

### Integration tests

Integration tests execute the real ingestion and pipeline against the Olist dataset.

They are marked with:

```python
@pytest.mark.integration
```

and can be executed with:

```bash
pytest -m integration
```

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

### Planned

- [ ] Delta Lake
- [ ] Databricks deployment
- [ ] Spark SQL transformations
- [ ] Incremental processing
- [ ] Production configuration
- [ ] Data orchestration
- [ ] Power BI semantic model
- [ ] Monitoring and observability

## Project Status

🚧 Project under development.