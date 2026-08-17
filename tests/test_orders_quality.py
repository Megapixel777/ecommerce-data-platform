from pyspark.sql import SparkSession

from src.quality.orders import check_orders_quality


def test_orders_quality_passes_for_valid_data():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-orders-quality")
        .getOrCreate()
    )

    orders = spark.createDataFrame(
        [
            ("order_1", "customer_1"),
            ("order_2", "customer_2"),
        ],
        ["order_id", "customer_id"],
    )

    customers = spark.createDataFrame(
        [
            ("customer_1",),
            ("customer_2",),
        ],
        ["customer_id"],
    )

    result = check_orders_quality(orders, customers)

    assert result == {
        "null_order_id": 0,
        "null_customer_id": 0,
        "duplicate_order_id": 0,
        "orphan_customer_id": 0,
    }

    spark.stop()