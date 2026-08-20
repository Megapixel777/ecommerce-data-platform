from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.ingestion.olist_bronze import load_orders


@pytest.mark.integration
def test_load_orders():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-olist-bronze")
        .getOrCreate()
    )

    input_path = Path(
        r"C:\Users\thoma\OneDrive\Desktop\olist\olist_orders_dataset.csv"
    )

    df = load_orders(spark, input_path)

    assert df.count() == 99441

    assert df.columns == [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    spark.stop()