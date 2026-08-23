from pyspark.sql import DataFrame

def transform_order_items(df: DataFrame) -> DataFrame:
    """
    Transform the order items DataFrame by filtering null and negative
    values in price and freight_value.
    """
    return (
        df.filter(df.price.isNotNull() & (df.price >= 0))
        .filter(df.freight_value.isNotNull() & (df.freight_value >= 0))
        .withColumn("total_item_value", df.price + df.freight_value)
    )