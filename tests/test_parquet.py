from pathlib import Path

from src.spark import create_spark_session


def test_parquet_write(tmp_path: Path):
    spark = create_spark_session()

    data = [
        ("A", 10.0),
        ("B", 20.0),
    ]

    df = spark.createDataFrame(
        data,
        ["order_id", "value"],
    )

    output_path = tmp_path / "parquet_test"

    df.write.mode("overwrite").parquet(str(output_path))

    result = spark.read.parquet(str(output_path))

    assert result.count() == 2

    spark.stop()
    