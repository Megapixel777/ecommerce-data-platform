import pytest
from pyspark.sql import SparkSession

from src.quality.orders import check_orders_quality, validate_quality_results


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-orders-quality")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_orders_quality_passes_for_valid_data(spark):
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


def test_orders_quality_detects_null_order_id(spark):
    orders = spark.createDataFrame(
        [
            (None, "customer_1"),
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

    assert result["null_order_id"] == 1


def test_orders_quality_detects_duplicate_order_id(spark):
    orders = spark.createDataFrame(
        [
            ("order_1", "customer_1"),
            ("order_1", "customer_2"),
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

    assert result["duplicate_order_id"] == 1


def test_orders_quality_detects_orphan_customer_id(spark):
    orders = spark.createDataFrame(
        [
            ("order_1", "customer_1"),
            ("order_2", "customer_999"),
        ],
        ["order_id", "customer_id"],
    )

    customers = spark.createDataFrame(
        [
            ("customer_1",),
        ],
        ["customer_id"],
    )

    result = check_orders_quality(orders, customers)

    assert result["orphan_customer_id"] == 1

def test_orders_quality_detects_null_customer_id(spark):
    orders = spark.createDataFrame(
        [
            ("order_1", None),
            ("order_2", "customer_2"),
        ],
        ["order_id", "customer_id"],
    )

    customers = spark.createDataFrame(
        [
            ("customer_2",),
        ],
        ["customer_id"],
    )

    result = check_orders_quality(orders, customers)

    assert result["null_customer_id"] == 1

def test_validate_quality_results_passes_for_valid_results():
    results = {
        "null_order_id": 0,
        "null_customer_id": 0,
        "duplicate_order_id": 0,
        "orphan_customer_id": 0,
    }

    validate_quality_results(results)

def test_validate_quality_results_fails_for_invalid_results():
    results = {
        "null_order_id": 1,
        "null_customer_id": 0,
        "duplicate_order_id": 0,
        "orphan_customer_id": 0,
    }

    with pytest.raises(ValueError, match="Data quality checks failed"):
        validate_quality_results(results)