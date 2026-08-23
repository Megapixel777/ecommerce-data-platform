from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_order_sales(df: DataFrame) -> DataFrame:
    """
    Build the order sales DataFrame by aggregating the order items data.

    Args:
        df (DataFrame): The input DataFrame containing order items data.

    Returns:
        DataFrame: The resulting DataFrame with aggregated order sales data.
    """

    # Group by order_id and calculate total_item_value
    result_df = (
        df.groupBy("order_id")
        .agg(F.sum("total_item_value").alias("total_order_value"),
             F.count("*").alias("item_count")
        )
    )

    return result_df