from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.silver.order_items import transform_order_items


def test_transform_order_items():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-order-items-silver")
        .getOrCreate()
    )

    data = [
    ("A", 10.0, 2.0, datetime(2017, 4, 23, 13, 25, 15)),
    ("B", -5.0, 2.0, datetime(2017, 4, 23, 13, 25, 15)),
    ("C", 10.0, -2.0, datetime(2017, 5, 10, 10, 0, 0)),
    ("D", None, 2.0, datetime(2017, 6, 15, 12, 0, 0)),
    ("E", 10.0, None, datetime(2017, 7, 20, 15, 0, 0)),
]

    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
            StructField("shipping_limit_date", TimestampType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    result = transform_order_items(df)

    row = result.first()

    assert result.count() == 1
    assert row["order_id"] == "A"
    assert row["total_item_value"] == 12.0
    assert row["shipping_year"] == 2017
    assert row["shipping_month"] == 4

    spark.stop()