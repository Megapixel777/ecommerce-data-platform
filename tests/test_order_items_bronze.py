import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from src.config import load_config
from src.ingestion.olist_bronze import load_order_items


@pytest.mark.integration
def test_load_order_items():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-olist-order-items")
        .getOrCreate()
    )

    input_path = (
        load_config().input_path
        / "olist_order_items_dataset.csv"
    )

    df = load_order_items(spark, input_path)

    assert df.count() == 112650

    assert df.columns == [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ]

    assert df.schema["order_item_id"].dataType == IntegerType()

    spark.stop()