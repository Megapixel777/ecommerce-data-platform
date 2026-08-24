from src.spark import create_spark_session


def test_create_spark_session():
    spark = create_spark_session()

    assert spark.version == "4.0.1"

    extensions = spark.conf.get("spark.sql.extensions")

    assert extensions is not None
    assert "DeltaSparkSessionExtension" in extensions

    catalog = spark.conf.get("spark.sql.catalog.spark_catalog")

    assert catalog == "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    spark.stop()