from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.utils.storage import write_parquet


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-storage")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_write_parquet_without_partition(spark, tmp_path):
    df = spark.createDataFrame(
        [
            ("A", 10.0),
            ("B", 20.0),
        ],
        ["order_id", "value"],
    )

    output_path = Path(tmp_path) / "output"

    write_parquet(df, output_path)

    result = spark.read.parquet(str(output_path))

    assert result.count() == 2
    assert set(result.columns) == {"order_id", "value"}


def test_write_parquet_with_partition(spark, tmp_path):
    df = spark.createDataFrame(
        [
            ("A", 2017, 10.0),
            ("B", 2017, 20.0),
            ("C", 2018, 30.0),
        ],
        ["order_id", "year", "value"],
    )

    output_path = Path(tmp_path) / "partitioned"

    write_parquet(
        df,
        output_path,
        partition_by=["year"],
    )

    result = spark.read.parquet(str(output_path))

    assert result.count() == 3
    assert "year" in result.columns

    assert (output_path / "year=2017").exists()
    assert (output_path / "year=2018").exists()