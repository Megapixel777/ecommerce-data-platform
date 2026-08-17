# E-Commerce Data Model

## 1. Purpose

This document defines the source data model for the E-Commerce Data Platform.

The initial historical dataset is based on the Brazilian Olist e-commerce dataset.

The platform will use the original Olist datasets as source data and will progressively extend the historical dataset with synthetic incremental batches to demonstrate scalable Data Engineering patterns.

The platform is designed to demonstrate:

- Batch ingestion
- Incremental processing
- PySpark transformations
- Delta Lake
- Data quality
- Deduplication
- Change Data Capture (CDC)
- Slowly Changing Dimensions Type 2 (SCD2)
- Data lineage
- SQL analytics
- CI/CD
- Databricks Jobs

---

## 2. Source Dataset

The initial source consists of nine CSV datasets.

| Source | Rows | Columns |
|---|---:|---:|
| customers | 99,441 | 5 |
| geolocation | 1,000,163 | 5 |
| orders | 99,441 | 8 |
| order_items | 112,650 | 7 |
| order_payments | 103,886 | 5 |
| order_reviews | 99,224 | 7 |
| products | 32,951 | 9 |
| sellers | 3,095 | 4 |
| category_translation | 71 | 2 |

The source files will remain outside the Git repository and will not be committed to GitHub.

---

## 3. Bronze Layer

The Bronze layer will preserve the source datasets with minimal transformation.

The following Delta tables will be created:

```text
main.ecommerce_bronze.customers
main.ecommerce_bronze.geolocation
main.ecommerce_bronze.orders
main.ecommerce_bronze.order_items
main.ecommerce_bronze.order_payments
main.ecommerce_bronze.order_reviews
main.ecommerce_bronze.products
main.ecommerce_bronze.sellers
main.ecommerce_bronze.product_category_translation