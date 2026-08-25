import json

from pyspark.sql.functions import col

from src.streaming.orders_stream import (
    aggregate_order_stream,
    read_order_stream,
    run_order_stream,
    write_aggregated_order_stream,
    write_order_stream,
)


def test_read_order_stream(spark, tmp_path):
    input_dir = tmp_path / "events"
    input_dir.mkdir()

    event = {
        "order_id": "ORDER-001",
        "customer_id": "CUSTOMER-001",
        "event_time": "2018-01-01 10:00:00",
        "status": "delivered",
        "value": 125.50,
    }

    with open(
        input_dir / "event_001.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(event, file)

    stream_df = read_order_stream(
        spark,
        str(input_dir),
    )

    assert stream_df.isStreaming
    assert stream_df.columns == [
        "order_id",
        "customer_id",
        "event_time",
        "status",
        "value",
    ]


def test_write_order_stream(spark, tmp_path):
    input_dir = tmp_path / "events"
    input_dir.mkdir()

    output_dir = tmp_path / "delta"
    checkpoint_dir = tmp_path / "checkpoint"

    event = {
        "order_id": "ORDER-001",
        "customer_id": "CUSTOMER-001",
        "event_time": "2018-01-01 10:05:00",
        "status": "delivered",
        "value": 125.50,
    }

    with open(
        input_dir / "event_001.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(event, file)

    stream_df = read_order_stream(
        spark,
        str(input_dir),
    )

    query = write_order_stream(
        stream_df,
        str(output_dir),
        str(checkpoint_dir),
    )

    query.awaitTermination()

    result = spark.read.format("delta").load(str(output_dir))

    assert result.count() == 1
    assert result.first()["order_id"] == "ORDER-001"


def test_aggregate_order_stream(spark):
    data = [
        (
            "ORDER-001",
            "CUSTOMER-001",
            "2018-01-01 10:01:00",
            "delivered",
            100.0,
        ),
        (
            "ORDER-002",
            "CUSTOMER-002",
            "2018-01-01 10:03:00",
            "delivered",
            50.0,
        ),
    ]

    stream_df = (
        spark.createDataFrame(
            data,
            [
                "order_id",
                "customer_id",
                "event_time",
                "status",
                "value",
            ],
        )
        .withColumn(
            "event_time",
            col("event_time").cast("timestamp"),
        )
    )

    result = aggregate_order_stream(stream_df)

    assert result.isStreaming is False
    assert result.columns == [
        "window",
        "orders_count",
        "total_revenue",
        "average_order_value",
    ]

    row = result.first()

    assert row["orders_count"] == 2
    assert row["total_revenue"] == 150.0
    assert row["average_order_value"] == 75.0

def test_write_aggregated_order_stream(spark, tmp_path):
    input_dir = tmp_path / "events"
    input_dir.mkdir()

    output_dir = tmp_path / "gold"
    checkpoint_dir = tmp_path / "gold_checkpoint"

    events = [
        {
            "order_id": "ORDER-001",
            "customer_id": "CUSTOMER-001",
            "event_time": "2018-01-01 10:01:00",
            "status": "delivered",
            "value": 100.0,
        },
        {
            "order_id": "ORDER-002",
            "customer_id": "CUSTOMER-002",
            "event_time": "2018-01-01 10:03:00",
            "status": "delivered",
            "value": 50.0,
        },
        {
            "order_id": "ORDER-003",
            "customer_id": "CUSTOMER-003",
            "event_time": "2018-01-01 10:20:00",
            "status": "delivered",
            "value": 25.0,
        },
    ]

    with open(
        input_dir / "events.json",
        "w",
        encoding="utf-8",
    ) as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

    stream_df = read_order_stream(
        spark,
        str(input_dir),
    )

    aggregated_df = aggregate_order_stream(stream_df)

    query = write_aggregated_order_stream(
        aggregated_df,
        str(output_dir),
        str(checkpoint_dir),
    )

    query.awaitTermination()

    result = spark.read.format("delta").load(str(output_dir))

    row = result.first()

    assert result.count() == 1
    assert row["orders_count"] == 2
    assert row["total_revenue"] == 150.0
    assert row["average_order_value"] == 75.0

def test_run_order_stream(spark, tmp_path):
    input_dir = tmp_path / "events"
    input_dir.mkdir()

    output_dir = tmp_path / "gold"
    checkpoint_dir = tmp_path / "checkpoint"

    events = [
        {
            "order_id": "ORDER-001",
            "customer_id": "CUSTOMER-001",
            "event_time": "2018-01-01 10:01:00",
            "status": "delivered",
            "value": 100.0,
        },
        {
            "order_id": "ORDER-002",
            "customer_id": "CUSTOMER-002",
            "event_time": "2018-01-01 10:03:00",
            "status": "delivered",
            "value": 50.0,
        },
        {
            "order_id": "ORDER-003",
            "customer_id": "CUSTOMER-003",
            "event_time": "2018-01-01 10:20:00",
            "status": "delivered",
            "value": 25.0,
        },
    ]

    with open(
        input_dir / "events.json",
        "w",
        encoding="utf-8",
    ) as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

    query = run_order_stream(
        spark,
        str(input_dir),
        str(output_dir),
        str(checkpoint_dir),
    )

    query.awaitTermination()

    result = spark.read.format("delta").load(str(output_dir))

    assert result.count() == 1

    row = result.first()

    assert row["orders_count"] == 2
    assert row["total_revenue"] == 150.0
    assert row["average_order_value"] == 75.0