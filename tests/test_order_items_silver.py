from pyspark.sql import SparkSession

from src.silver.order_items import transform_order_items



def test_transform_order_items():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-order-items-silver")
        .getOrCreate()
    )

    data = [
        ("A", 10.0, 2.0),
        ("B", -5.0, 2.0),
        ("C", 10.0, -2.0),
        ("D", None, 2.0),
        ("E", 10.0, None),
    ]
    df = spark.createDataFrame(data, ["order_id", "price", "freight_value"])

    result = transform_order_items(df)

    assert result.count() == 1
    assert result.first().order_id == "A"
    assert result.first().total_item_value == 12.0

    spark.stop()