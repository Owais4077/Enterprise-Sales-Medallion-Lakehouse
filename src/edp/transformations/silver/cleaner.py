"""Silver cleaner: cast types, enforce business rules, and split quarantine rows."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StructField,
    TimestampType,
)

from edp.transformations.silver.schemas import DatasetSilverSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CleanedBatch:
    dataset: str
    valid_df: DataFrame
    quarantine_df: DataFrame
    total_records: int
    valid_records: int
    quarantined_records: int


def _cast_column(col_name: str, target_field: StructField) -> Column:
    dt = target_field.dataType
    c = F.col(col_name)
    if isinstance(dt, LongType):
        return c.cast("long")
    elif isinstance(dt, IntegerType):
        return c.cast("integer")
    elif isinstance(dt, DecimalType):
        return c.cast(f"decimal({dt.precision},{dt.scale})")
    elif isinstance(dt, TimestampType):
        return F.to_timestamp(c)
    elif isinstance(dt, DateType):
        return F.to_date(c)
    elif isinstance(dt, BooleanType):
        return c.cast("boolean")
    else:
        return c.cast("string")


def cast_bronze_df(df: DataFrame, spec: DatasetSilverSpec) -> DataFrame:
    """Safely cast raw text columns into their target Silver schema types."""
    target_fields = {field.name: field for field in spec.target_schema.fields}

    select_exprs = []
    for col_name in df.columns:
        if col_name in target_fields:
            select_exprs.append(_cast_column(col_name, target_fields[col_name]).alias(col_name))
        else:
            select_exprs.append(F.col(col_name))

    return df.select(*select_exprs)


def build_validation_conditions(df: DataFrame, spec: DatasetSilverSpec) -> list[tuple[Column, str]]:
    """Returns a list of (condition, reason_message) pairs where True means VALID."""
    conditions: list[tuple[Column, str]] = []

    # 1. Primary key checks (must not be null)
    for pk_col in spec.primary_key:
        if pk_col in df.columns:
            conditions.append((F.col(pk_col).isNotNull(), f"Primary key {pk_col} is null"))

    # 2. Dataset-specific business rules
    if spec.dataset == "customers":
        if "email" in df.columns:
            conditions.append(
                (
                    F.col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"),
                    "Invalid email format",
                )
            )
        if "created_at" in df.columns and "updated_at" in df.columns:
            conditions.append(
                (F.col("updated_at") >= F.col("created_at"), "updated_at prior to created_at")
            )

    elif spec.dataset == "products":
        if "unit_price" in df.columns:
            conditions.append((F.col("unit_price") >= 0, "Negative unit_price"))
        if "unit_cost" in df.columns:
            conditions.append((F.col("unit_cost") >= 0, "Negative unit_cost"))

    elif spec.dataset == "orders":
        if "total_amount" in df.columns:
            conditions.append((F.col("total_amount") >= 0, "Negative total_amount"))
        if "status" in df.columns:
            valid_statuses = ("pending", "paid", "shipped", "delivered", "cancelled", "returned")
            conditions.append((F.col("status").isin(*valid_statuses), "Invalid order status"))

    elif spec.dataset == "order_items":
        if "quantity" in df.columns:
            conditions.append((F.col("quantity") > 0, "Non-positive quantity"))
        if "unit_price" in df.columns:
            conditions.append((F.col("unit_price") >= 0, "Negative unit_price"))
        if "discount_pct" in df.columns:
            conditions.append(
                (
                    (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 100),
                    "Discount pct out of 0-100 range",
                )
            )

    elif spec.dataset == "payments":
        if "amount" in df.columns:
            conditions.append((F.col("amount") >= 0, "Negative payment amount"))
        if "payment_status" in df.columns:
            valid_statuses = ("pending", "completed", "failed", "refunded")
            conditions.append(
                (F.col("payment_status").isin(*valid_statuses), "Invalid payment status")
            )

    elif spec.dataset == "reviews":
        if "rating" in df.columns:
            conditions.append(
                ((F.col("rating") >= 1) & (F.col("rating") <= 5), "Rating outside 1-5 range")
            )

    return conditions


def clean_and_split(df: DataFrame, spec: DatasetSilverSpec) -> tuple[DataFrame, DataFrame]:
    """Casts raw Bronze DataFrame into typed schema and splits into (valid_df, quarantine_df)."""
    typed_df = cast_bronze_df(df, spec)
    rules = build_validation_conditions(typed_df, spec)

    if not rules:
        valid_df = typed_df
        quarantine_df = typed_df.limit(0).withColumn("quarantine_reason", F.lit("").cast("string"))
        return valid_df, quarantine_df

    # Construct composite validation flag & array of reasons
    reasons_array = F.array(
        *[F.when(~cond, F.lit(msg)).otherwise(F.lit(None)) for cond, msg in rules]
    )
    reasons_filtered = F.array_compact(reasons_array)

    df_with_reasons = typed_df.withColumn("_reasons", reasons_filtered)
    is_valid_col = F.size(F.col("_reasons")) == 0

    valid_df = df_with_reasons.filter(is_valid_col).drop("_reasons")
    quarantine_df = (
        df_with_reasons.filter(~is_valid_col)
        .withColumn("quarantine_reason", F.array_join(F.col("_reasons"), "; "))
        .withColumn("quarantined_at", F.current_timestamp())
        .drop("_reasons")
    )

    return valid_df, quarantine_df
