import pytest
from pyspark.sql import SparkSession

from src.gold.kpis import build_kpis


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-kpis-gold")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_build_kpis(spark):
    data = [
        ("A", 30.0, 2),
        ("B", 50.0, 3),
    ]

    order_sales = spark.createDataFrame(
        data,
        ["order_id", "total_order_value", "item_count"],
    )

    result = build_kpis(order_sales).first()

    assert result["total_orders"] == 2
    assert result["total_revenue"] == 80.0
    assert result["average_order_value"] == 40.0
    assert result["total_items"] == 5