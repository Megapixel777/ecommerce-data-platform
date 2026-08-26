import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    sum,
    window,
)

from src.config import load_config
from src.spark import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def read_order_stream(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    """Read order events as a Spark Structured Streaming DataFrame."""

    return (
        spark.readStream
        .format("json")
        .schema(
            """
            order_id STRING,
            customer_id STRING,
            event_time TIMESTAMP,
            status STRING,
            value DOUBLE
            """
        )
        .option("maxFilesPerTrigger", 1)
        .load(input_path)
    )


def aggregate_order_stream(
    stream_df: DataFrame,
) -> DataFrame:
    """Aggregate orders into five-minute event-time windows."""

    return (
        stream_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window("event_time", "5 minutes"),
        )
        .agg(
            count("order_id").alias("orders_count"),
            sum("value").alias("total_revenue"),
            avg("value").alias("average_order_value"),
        )
    )


def write_aggregated_order_stream(
    aggregated_df: DataFrame,
    output_path: str,
    checkpoint_path: str,
):
    """Write aggregated order metrics to Delta."""

    return (
        aggregated_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(output_path)
    )


def run_order_stream(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    checkpoint_path: str,
):
    """Run the complete order streaming pipeline."""

    stream_df = read_order_stream(
        spark,
        input_path,
    )

    aggregated_df = aggregate_order_stream(
        stream_df,
    )

    return write_aggregated_order_stream(
        aggregated_df,
        output_path,
        checkpoint_path,
    )


def main() -> None:
    config = load_config()

    parser = argparse.ArgumentParser(
        description="Run the Olist order streaming pipeline."
    )

    parser.add_argument(
        "--input-path",
        default=None,
        help="Path to the streaming events directory.",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory where streaming outputs will be written.",
    )

    parser.add_argument(
        "--checkpoint-dir",
        default=None,
        help="Directory where the streaming checkpoint will be stored.",
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

    checkpoint_dir = (
        Path(args.checkpoint_dir)
        if args.checkpoint_dir
        else output_dir / "checkpoints" / "orders"
    )

    logger.info("Environment: %s", config.environment)
    logger.info("Streaming input path: %s", input_path)
    logger.info("Streaming output path: %s", output_dir)
    logger.info("Streaming checkpoint path: %s", checkpoint_dir)

    spark = create_spark_session()

    try:
        query = run_order_stream(
            spark,
            str(input_path),
            str(output_dir / "gold" / "streaming_order_metrics"),
            str(checkpoint_dir),
        )

        query.awaitTermination()

        logger.info("Streaming pipeline completed successfully")

    except Exception:
        logger.exception("Streaming pipeline execution failed")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()