from pathlib import Path

from src.spark import create_spark_session


def test_delta_spark_session(tmp_path: Path):
    spark = create_spark_session()

    data = [
        ("A", 10.0),
        ("B", 20.0),
    ]

    df = spark.createDataFrame(
        data,
        ["order_id", "value"],
    )

    output_path = tmp_path / "delta_test"

    df.write.format("delta").mode("overwrite").save(str(output_path))

    result = spark.read.format("delta").load(str(output_path))

    assert result.count() == 2
    assert "order_id" in result.columns
    assert "value" in result.columns

    spark.stop()