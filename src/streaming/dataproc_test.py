from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum

BUCKET = "gs://ecommerce-data-platform-gen-lang-client-0097541881"

spark = SparkSession.builder.appName("DataprocOrdersTest").getOrCreate()

orders = (
    spark.read
    .json(f"{BUCKET}/raw/orders/*.json")
)

print("=== SCHEMA ===")
orders.printSchema()

print("=== ORDERS ===")
orders.show(truncate=False)

print("=== AGGREGATION ===")

result = (
    orders
    .groupBy("status")
    .agg(
        count("order_id").alias("orders_count"),
        sum("value").alias("total_revenue"),
        avg("value").alias("average_order_value"),
    )
)

result.show(truncate=False)

print("=== WRITING TO BIGQUERY ===")

(
    result.write
    .format("bigquery")
    .option(
        "table",
        "gen-lang-client-0097541881.ecommerce.orders_summary",
    )
    .option(
        "temporaryGcsBucket",
        BUCKET.replace("gs://", ""),
    )
    .mode("overwrite")
    .save()
)

print("=== BIGQUERY WRITE COMPLETE ===")

spark.stop()