from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.ingestion.olist_bronze import load_customers


@pytest.mark.integration
def test_load_customers():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-olist-customers")
        .getOrCreate()
    )

    input_path = Path(
        r"C:\Users\thoma\OneDrive\Desktop\olist\olist_customers_dataset.csv"
    )

    df = load_customers(spark, input_path)

    assert df.count() == 99441

    assert df.columns == [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ]

    spark.stop()