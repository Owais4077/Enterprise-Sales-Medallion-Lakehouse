"""Silver layer schemas and dataset primary key definitions."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

METADATA_FIELDS = [
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_system", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("run_id", StringType(), True),
]


@dataclass(frozen=True)
class DatasetSilverSpec:
    dataset: str
    primary_key: tuple[str, ...]
    watermark_col: str | None
    target_schema: StructType


SILVER_DATASET_SPECS: dict[str, DatasetSilverSpec] = {
    "customers": DatasetSilverSpec(
        dataset="customers",
        primary_key=("customer_id",),
        watermark_col="updated_at",
        target_schema=StructType(
            [
                StructField("customer_id", LongType(), False),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("city", StringType(), True),
                StructField("segment", StringType(), True),
                StructField("signup_date", DateType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
    "products": DatasetSilverSpec(
        dataset="products",
        primary_key=("product_id",),
        watermark_col="updated_at",
        target_schema=StructType(
            [
                StructField("product_id", LongType(), False),
                StructField("sku", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("unit_price", DecimalType(12, 2), True),
                StructField("unit_cost", DecimalType(12, 2), True),
                StructField("currency", StringType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
    "orders": DatasetSilverSpec(
        dataset="orders",
        primary_key=("order_id",),
        watermark_col="updated_at",
        target_schema=StructType(
            [
                StructField("order_id", LongType(), False),
                StructField("customer_id", LongType(), True),
                StructField("order_date", TimestampType(), True),
                StructField("status", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("shipping_country", StringType(), True),
                StructField("total_amount", DecimalType(14, 2), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
    "order_items": DatasetSilverSpec(
        dataset="order_items",
        primary_key=("order_item_id",),
        watermark_col="updated_at",
        target_schema=StructType(
            [
                StructField("order_item_id", LongType(), False),
                StructField("order_id", LongType(), True),
                StructField("product_id", LongType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DecimalType(12, 2), True),
                StructField("discount_pct", DecimalType(5, 2), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
    "payments": DatasetSilverSpec(
        dataset="payments",
        primary_key=("payment_id",),
        watermark_col="updated_at",
        target_schema=StructType(
            [
                StructField("payment_id", LongType(), False),
                StructField("order_id", LongType(), True),
                StructField("payment_method", StringType(), True),
                StructField("payment_status", StringType(), True),
                StructField("amount", DecimalType(14, 2), True),
                StructField("currency", StringType(), True),
                StructField("paid_at", TimestampType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
    "exchange_rates": DatasetSilverSpec(
        dataset="exchange_rates",
        primary_key=("rate_date", "quote_currency"),
        watermark_col="updated_at",
        target_schema=StructType(
            [
                StructField("rate_date", DateType(), False),
                StructField("quote_currency", StringType(), False),
                StructField("rate_to_usd", DecimalType(12, 6), True),
                StructField("updated_at", TimestampType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
    "countries": DatasetSilverSpec(
        dataset="countries",
        primary_key=("country_code",),
        watermark_col=None,
        target_schema=StructType(
            [
                StructField("country_code", StringType(), False),
                StructField("iso3", StringType(), True),
                StructField("country_name", StringType(), True),
                StructField("region", StringType(), True),
                StructField("currency_code", StringType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
    "reviews": DatasetSilverSpec(
        dataset="reviews",
        primary_key=("review_id",),
        watermark_col=None,
        target_schema=StructType(
            [
                StructField("review_id", StringType(), False),
                StructField("product_id", LongType(), True),
                StructField("customer_id", LongType(), True),
                StructField("rating", IntegerType(), True),
                StructField("comment", StringType(), True),
                StructField("review_date", TimestampType(), True),
                *METADATA_FIELDS,
            ]
        ),
    ),
}
