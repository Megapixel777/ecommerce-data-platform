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