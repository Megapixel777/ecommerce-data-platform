import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.gold.kpis import build_kpis
from src.gold.order_sales import build_order_sales
from src.ingestion.olist_bronze import load_order_items
from src.silver.order_items import transform_order_items
from src.utils.storage import write_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def run_pipeline(
    spark: SparkSession,
    input_path: str | Path,
    output_dir: str | Path,
) -> DataFrame:
    """Run the Olist order items pipeline from Bronze to Gold."""

    output_dir = Path(output_dir)

    logger.info("Starting Olist pipeline")
    logger.info("Input path: %s", input_path)
    logger.info("Output directory: %s", output_dir)

    logger.info("Loading Bronze data")
    bronze = load_order_items(spark, input_path)
    write_parquet(bronze, output_dir / "bronze" / "order_items")

    logger.info("Transforming Silver data")
    silver = transform_order_items(bronze)
    write_parquet(
        silver,
        output_dir / "silver" / "order_items",
        partition_by=["shipping_year", "shipping_month"],
    )

    logger.info("Building Gold order sales")
    order_sales = build_order_sales(silver)
    write_parquet(order_sales, output_dir / "gold" / "order_sales")

    logger.info("Building Gold KPIs")
    kpis = build_kpis(order_sales)
    write_parquet(kpis, output_dir / "gold" / "kpis")

    logger.info("Pipeline completed successfully")

    return kpis


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Olist order items pipeline."
    )

    parser.add_argument(
        "--input-path",
        required=True,
        help="Path to the Olist order items CSV.",
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where pipeline outputs will be written.",
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("olist-pipeline")
        .getOrCreate()
    )

    try:
        run_pipeline(
            spark,
            args.input_path,
            args.output_dir,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()