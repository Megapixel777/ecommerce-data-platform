from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.gold.kpis import build_kpis
from src.gold.order_sales import build_order_sales
from src.ingestion.olist_bronze import load_order_items
from src.silver.order_items import transform_order_items


def run_pipeline(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    """Run the Olist order items pipeline from Bronze to Gold."""

    bronze = load_order_items(spark, input_path)

    silver = transform_order_items(bronze)

    order_sales = build_order_sales(silver)

    kpis = build_kpis(order_sales)

    return kpis