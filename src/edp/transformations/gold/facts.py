"""Gold layer fact table transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_sales(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    exchange_rates_df: DataFrame | None = None,
) -> DataFrame:
    """Builds line-item fact_sales table with USD currency conversions."""
    orders_slim = orders_df.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_date"),
        F.date_format("order_date", "yyyyMMdd").cast("integer").alias("order_date_key"),
        F.to_date("order_date").alias("rate_date"),
        F.col("status").alias("order_status"),
        F.col("currency"),
        F.col("shipping_country"),
    )

    joined = order_items_df.join(orders_slim, on="order_id", how="inner")

    if exchange_rates_df is not None:
        rates_slim = exchange_rates_df.select(
            F.col("rate_date"),
            F.col("quote_currency").alias("currency"),
            F.col("rate_to_usd"),
        )
        joined = joined.join(rates_slim, on=["rate_date", "currency"], how="left")

    rate_col = (
        F.when(F.col("currency") == "USD", F.lit(1.0))
        .when(F.col("rate_to_usd").isNotNull(), F.col("rate_to_usd"))
        .otherwise(F.lit(1.0))
    )

    line_total_local = (
        F.col("quantity")
        * F.col("unit_price")
        * (F.lit(1.0) - (F.coalesce(F.col("discount_pct"), F.lit(0.0)) / F.lit(100.0)))
    )
    line_total_usd = line_total_local * rate_col

    return joined.select(
        F.concat_ws("_", F.col("order_id"), F.col("order_item_id")).alias("sales_fact_key"),
        F.col("order_id"),
        F.col("order_item_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("order_date_key"),
        F.col("shipping_country"),
        F.col("order_status"),
        F.col("currency"),
        F.col("quantity"),
        F.col("unit_price").alias("unit_price_local"),
        F.coalesce(F.col("discount_pct"), F.lit(0.0)).alias("discount_pct"),
        line_total_local.cast("decimal(14,2)").alias("line_total_local"),
        rate_col.cast("decimal(12,6)").alias("exchange_rate_to_usd"),
        line_total_usd.cast("decimal(14,2)").alias("line_total_usd"),
        F.current_timestamp().alias("updated_at"),
    )
