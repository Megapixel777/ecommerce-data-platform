# Ecommerce Data Model

## Overview

This document describes the data model used by the e-commerce data platform.

The model is designed to support:

- Customer analysis
- Product and catalog analysis
- Order and sales analysis
- Payment analysis
- Delivery and logistics analysis
- Business and operational reporting

The data model follows a dimensional approach, separating measurable business events from descriptive entities.

## Data Domains

The platform is organized around the following main domains:

- Customers
- Products
- Orders
- Order Items
- Payments
- Shipping
- Reviews

## Core Entities

### Customer

Represents a customer registered on the e-commerce platform.

Typical attributes:

- `customer_id`
- `customer_unique_id`
- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

### Product

Represents a product available through the platform.

Typical attributes:

- `product_id`
- `product_category`
- `product_name_length`
- `product_description_length`
- `product_photos_qty`
- `product_weight_g`
- `product_length_cm`
- `product_height_cm`
- `product_width_cm`

### Order

Represents a customer order.

Typical attributes:

- `order_id`
- `customer_id`
- `order_status`
- `order_purchase_timestamp`
- `order_approved_at`
- `order_delivered_carrier_date`
- `order_delivered_customer_date`
- `order_estimated_delivery_date`

### Order Item

Represents a product included in an order.

Typical attributes:

- `order_id`
- `order_item_id`
- `product_id`
- `seller_id`
- `shipping_limit_date`
- `price`
- `freight_value`

### Payment

Represents a payment associated with an order.

Typical attributes:

- `order_id`
- `payment_sequential`
- `payment_type`
- `payment_installments`
- `payment_value`

### Review

Represents a customer review associated with an order.

Typical attributes:

- `review_id`
- `order_id`
- `review_score`
- `review_comment_title`
- `review_comment_message`
- `review_creation_date`
- `review_answer_timestamp`

## Relationships

The main relationships are:

```text
Customer
   │
   └──< Order
          │
          ├──< Order Item >── Product
          │        │
          │        └── Seller
          │
          ├──< Payment
          │
          └──< Review