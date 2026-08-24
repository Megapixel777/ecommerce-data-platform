from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_orders_quality(orders: DataFrame, customers: DataFrame) -> dict[str, int]:
    """Run data quality checks for the orders dataset."""

    null_order_id = orders.filter(col("order_id").isNull()).count()

    null_customer_id = orders.filter(col("customer_id").isNull()).count()

    total_orders = orders.count()
    distinct_orders = orders.select("order_id").distinct().count()
    duplicate_orders = total_orders - distinct_orders

    orphan_orders = (
        orders.select("customer_id")
        .distinct()
        .join(
            customers.select("customer_id").distinct(),
            on="customer_id",
            how="left_anti",
        )
        .count()
    )

    return {
        "null_order_id": null_order_id,
        "null_customer_id": null_customer_id,
        "duplicate_order_id": duplicate_orders,
        "orphan_customer_id": orphan_orders,
    }


def validate_quality_results(results: dict[str, int]) -> None:
    """Raise an error when critical data quality checks fail."""

    critical_checks = {
        "null_order_id",
        "null_customer_id",
        "duplicate_order_id",
        "orphan_customer_id",
    }

    failures = {
        check: value
        for check, value in results.items()
        if check in critical_checks and value > 0
    }

    if failures:
        raise ValueError(
            f"Data quality checks failed: {failures}"
        )