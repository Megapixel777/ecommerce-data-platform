from pyspark.sql import SparkSession

from src.gold.order_sales import build_order_sales


def test_build_order_sales():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-order-sales-gold")
        .getOrCreate()
    )

    data = [
        ("A", 10.0),
        ("A", 20.0),
        ("B", 15.0),
        ("B", 25.0),
        ("B", 10.0),
    ]

    df = spark.createDataFrame(
        data,
        ["order_id", "total_item_value"],
    )

    result = build_order_sales(df)

    rows = {
        row["order_id"]: (
            row["total_order_value"],
            row["item_count"],
        )
        for row in result.collect()
    }

    assert rows["A"] == (30.0, 2)
    assert rows["B"] == (50.0, 3)

    spark.stop()