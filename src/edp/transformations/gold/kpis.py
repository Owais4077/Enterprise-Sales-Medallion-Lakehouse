"""Gold layer KPI aggregation transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_kpi_monthly_sales(
    fact_sales_df: DataFrame, dim_product_df: DataFrame | None = None
) -> DataFrame:
    """Aggregates sales metrics by year, month, country, and category."""
    df = fact_sales_df.withColumn(
        "year", (F.col("order_date_key") / 10000).cast("integer")
    ).withColumn("month", ((F.col("order_date_key") % 10000) / 100).cast("integer"))

    if dim_product_df is not None:
        p_slim = dim_product_df.select(F.col("product_id"), F.col("category"))
        df = df.join(p_slim, on="product_id", how="left")
    else:
        df = df.withColumn("category", F.lit("unknown"))

    return df.groupBy("year", "month", "shipping_country", "category").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").cast("integer").alias("total_items_sold"),
        F.sum("line_total_usd").cast("decimal(16,2)").alias("total_revenue_usd"),
        F.current_timestamp().alias("updated_at"),
    )
