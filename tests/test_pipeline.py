
import pytest
from pyspark.sql import SparkSession

from src.config import load_config
from src.pipeline import run_pipeline


@pytest.mark.integration
def test_run_pipeline(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-olist-pipeline")
        .getOrCreate()
    )

    input_path = load_config().input_path

    output_dir = tmp_path / "pipeline_output"

    result = run_pipeline(
        spark,
        input_path,
        output_dir,
    )

    row = result.first()

    assert result.count() == 1
    assert row["total_orders"] > 0
    assert row["total_revenue"] > 0
    assert row["average_order_value"] > 0
    assert row["total_items"] > 0

    silver_output = output_dir / "silver" / "order_items"

    assert any(
        path.name.startswith("shipping_year=")
        for path in silver_output.iterdir()
    )

    spark.stop()