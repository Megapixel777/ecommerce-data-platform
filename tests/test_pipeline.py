import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.pipeline import run_pipeline

load_dotenv()

@pytest.mark.integration
def test_run_pipeline():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-olist-pipeline")
        .getOrCreate()
    )

    input_path = (
        Path(os.environ["OLIST_DATA_PATH"])
        / "olist_order_items_dataset.csv"
    )

    output_dir = Path("tests/test_output")

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

    spark.stop()