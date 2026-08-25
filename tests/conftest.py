import pytest

from src.spark import create_spark_session


@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session()

    yield spark

    spark.stop()