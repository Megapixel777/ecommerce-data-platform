# ADR-001 — Medallion Architecture

* **Status:** Accepted
* **Date:** 2026-08-17

## Context

The project requires an architecture that separates raw ingested data from cleaned, validated and business-ready data.

The platform will process multiple e-commerce data sources, including customers, products, orders, payments, returns and web events.

We also want the architecture to support data quality, incremental processing, auditability and future scalability.

## Decision

We will use a **Medallion Architecture** consisting of three layers:

* **Bronze:** raw ingested data with minimal transformation.
* **Silver:** cleaned, standardized, deduplicated and validated data.
* **Gold:** business-ready datasets and data marts designed for analytics and reporting.

Within Databricks Unity Catalog, each layer will be represented by a dedicated schema:

```text
main.ecommerce_bronze
main.ecommerce_silver
main.ecommerce_gold
```

Tables will therefore follow the Unity Catalog three-level namespace:

```text
catalog.schema.table
```

For example:

```text
main.ecommerce_bronze.orders
main.ecommerce_silver.orders
main.ecommerce_gold.sales_daily
```

## Rationale

The Bronze layer provides a reliable representation of the ingested source data and allows the pipeline to retain ingestion metadata and support traceability.

The Silver layer provides a controlled and consistent dataset where data quality rules, deduplication, standardization and business transformations can be applied.

The Gold layer separates business-facing models from the underlying engineering layers, allowing analytical datasets to be designed specifically for reporting and consumption.

Using separate Unity Catalog schemas provides clear logical isolation between the three processing stages and makes the architecture immediately visible through the catalog structure.

## Consequences

### Positive

* Clear separation of processing responsibilities.
* Improved data lineage and traceability.
* Easier implementation of data quality controls.
* Clear separation between engineering and business-facing datasets.
* The architecture can evolve as the volume and complexity of the data increase.
* The structure is easy to understand and maintain.

### Negative

* Data may be stored in multiple layers, increasing storage requirements.
* Pipelines become more complex than a single-layer architecture.
* Data may be processed multiple times between layers.

## Alternatives Considered

### Single-layer architecture

Rejected because it would mix raw, transformed and business-ready data, making data quality, lineage and maintenance more difficult.

### Single schema with naming conventions

For example:

```text
main.ecommerce.bronze_orders
main.ecommerce.silver_orders
main.ecommerce.gold_sales_daily
```

Rejected because separate schemas provide clearer logical isolation between processing layers.

## Future Considerations

The architecture may be extended with additional environments or catalogs if the project requirements grow.

Incremental processing, Delta Lake features, data quality controls, CI/CD and orchestration will be documented in subsequent ADRs as those components are implemented.
