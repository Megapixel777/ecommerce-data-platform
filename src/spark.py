import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Create a SparkSession configured for Delta Lake."""

    existing_session = SparkSession.getActiveSession()

    if existing_session is not None:
        existing_session.stop()

    python_executable = os.environ.get(
        "PYSPARK_PYTHON",
        "C:/Users/thoma/anaconda3/envs/ecommerce-data-platform-py311/python.exe",
    )

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-data-platform")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()