import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.pipeline import run_pipeline

load_dotenv()


@pytest.mark.integration
def test_run_pipeline(tmp_path):
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-olist-pipeline")
        .getOrCreate()
    )

    input_path = Path(os.environ["OLIST_DATA_PATH"])

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