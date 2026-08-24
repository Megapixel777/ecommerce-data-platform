import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config import load_config
from src.gold.kpis import build_kpis
from src.gold.order_sales import build_order_sales
from src.ingestion.olist_bronze import (
    load_customers,
    load_order_items,
    load_orders,
)
from src.quality.orders import check_orders_quality, validate_quality_results
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

    input_path = Path(input_path)
    output_dir = Path(output_dir)

    logger.info("Starting Olist pipeline")
    logger.info("Input path: %s", input_path)
    logger.info("Output directory: %s", output_dir)

    orders_path = input_path / "olist_orders_dataset.csv"
    customers_path = input_path / "olist_customers_dataset.csv"
    order_items_path = input_path / "olist_order_items_dataset.csv"

    logger.info("Loading Orders data")
    orders = load_orders(spark, orders_path)

    logger.info("Loading Customers data")
    customers = load_customers(spark, customers_path)

    logger.info("Running Orders data quality checks")
    quality_results = check_orders_quality(orders, customers)

    for check, result in quality_results.items():
        logger.info("Data quality - %s: %s", check, result)

    validate_quality_results(quality_results)
    logger.info("Data quality checks passed")

    logger.info("Loading Bronze order items")
    bronze = load_order_items(spark, order_items_path)
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
    config = load_config()

    parser = argparse.ArgumentParser(
        description="Run the Olist order items pipeline."
    )

    parser.add_argument(
        "--input-path",
        default=None,
        help="Path to the Olist dataset directory.",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory where pipeline outputs will be written.",
    )

    args = parser.parse_args()

    input_path = (
        Path(args.input_path)
        if args.input_path
        else config.input_path
    )

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else config.output_dir
    )

    logger.info("Environment: %s", config.environment)

    spark = (
        SparkSession.builder
        .appName("olist-pipeline")
        .getOrCreate()
    )

    try:
        run_pipeline(
            spark,
            input_path,
            output_dir,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()