from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def load_csv(
    spark: SparkSession,
    input_path: str | Path,
    schema: StructType,
) -> DataFrame:
    """Load a CSV file using an explicit Spark schema."""

    path = Path(input_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if not path.is_file():
        raise ValueError(f"Input path is not a file: {path}")

    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(str(path))
    )