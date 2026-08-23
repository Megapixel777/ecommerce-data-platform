from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_kpis(order_sales: DataFrame) -> DataFrame:
    """Build overall order and revenue KPIs."""
    return order_sales.agg(
        F.count("*").alias("total_orders"),
        F.sum("total_order_value").alias("total_revenue"),
        F.avg("total_order_value").alias("average_order_value"),
        F.sum("item_count").alias("total_items"),
    )