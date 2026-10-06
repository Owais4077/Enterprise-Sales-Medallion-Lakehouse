"""Gold layer dimension table transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_dim_customer(customers_df: DataFrame) -> DataFrame:
    """Builds dim_customer from Silver customers."""
    return customers_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
        F.col("email"),
        F.col("phone"),
        F.col("country_code"),
        F.col("city"),
        F.col("segment"),
        F.col("signup_date"),
        F.col("is_active"),
        F.current_timestamp().alias("updated_at"),
    ).distinct()


def build_dim_product(products_df: DataFrame) -> DataFrame:
    """Builds dim_product from Silver products."""
    return products_df.select(
        F.col("product_id"),
        F.col("sku"),
        F.col("product_name"),
        F.col("category"),
        F.col("unit_price"),
        F.col("unit_cost"),
        F.col("currency"),
        F.col("is_active"),
        F.current_timestamp().alias("updated_at"),
    ).distinct()


def build_dim_country(countries_df: DataFrame) -> DataFrame:
    """Builds dim_country from Silver countries."""
    return countries_df.select(
        F.col("country_code"),
        F.col("iso3"),
        F.col("country_name"),
        F.col("region"),
        F.col("currency_code"),
        F.current_timestamp().alias("updated_at"),
    ).distinct()


def build_dim_date(
    spark: SparkSession, start_date: str = "2020-01-01", end_date: str = "2030-12-31"
) -> DataFrame:
    """Generates a complete date dimension DataFrame."""
    query = (
        f"SELECT explode(sequence(to_date('{start_date}'), "
        f"to_date('{end_date}'), interval 1 day)) as full_date"
    )
    dates_df = spark.sql(query)

    return dates_df.select(
        F.date_format("full_date", "yyyyMMdd").cast("integer").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )
