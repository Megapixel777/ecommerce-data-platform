import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.pipeline import run_pipeline

load_dotenv() #To load the environment variables from the .env file

def test_run_pipeline():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-olist-pipeline")
        .getOrCreate()
    )

    input_path = Path(
    os.environ["OLIST_DATA_PATH"]
) / "olist_order_items_dataset.csv"

    result = run_pipeline(spark, input_path)

    row = result.first()

    assert result.count() == 1
    assert row["total_orders"] > 0
    assert row["total_revenue"] > 0
    assert row["average_order_value"] > 0
    assert row["total_items"] > 0

    spark.stop()