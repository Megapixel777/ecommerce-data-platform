from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.ingestion.loader import load_csv
from src.ingestion.schemas import CUSTOMERS_SCHEMA, ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA


def load_orders(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    """Load Olist orders CSV using the explicit orders schema."""

    return load_csv(
        spark,
        input_path,
        ORDERS_SCHEMA,
    )

def load_customers(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    """Load Olist customers CSV using the explicit customers schema."""

    return load_csv(
        spark,
        input_path,
        CUSTOMERS_SCHEMA,
    )

def load_order_items(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    """Load Olist order items CSV using the explicit order items schema."""

    return load_csv(
        spark,
        input_path,
        ORDER_ITEMS_SCHEMA,
    )