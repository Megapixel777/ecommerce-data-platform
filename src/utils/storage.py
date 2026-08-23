from pathlib import Path

from pyspark.sql import DataFrame


def write_parquet(
    df: DataFrame,
    output_path: str | Path,
    partition_by: list[str] | None = None,
) -> None:
    """Write a DataFrame to Parquet using overwrite mode."""

    writer = df.write.mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(str(output_path))