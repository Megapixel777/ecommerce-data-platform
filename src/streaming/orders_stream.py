from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    sum,
    window,
)


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

def write_order_stream(
    stream_df: DataFrame,
    output_path: str,
    checkpoint_path: str,
):
    """Write order events to Delta using Structured Streaming."""

    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(output_path)
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