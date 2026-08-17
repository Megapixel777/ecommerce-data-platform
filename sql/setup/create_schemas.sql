-- E-Commerce Data Platform
-- Unity Catalog schema initialization

CREATE SCHEMA IF NOT EXISTS main.ecommerce_bronze
COMMENT 'Bronze layer - raw ingested e-commerce data';

CREATE SCHEMA IF NOT EXISTS main.ecommerce_silver
COMMENT 'Silver layer - cleaned, validated and enriched e-commerce data';

CREATE SCHEMA IF NOT EXISTS main.ecommerce_gold
COMMENT 'Gold layer - business-ready e-commerce data marts';