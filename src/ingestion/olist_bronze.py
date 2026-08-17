from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.ingestion.schemas import ORDERS_SCHEMA


def load_orders(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    """Load Olist orders CSV using the explicit orders schema."""

    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if not path.is_file():
        raise ValueError(f"Input path is not a file: {path}")

    return (
        spark.read
        .option("header", True)
        .schema(ORDERS_SCHEMA)
        .csv(str(path))
    )