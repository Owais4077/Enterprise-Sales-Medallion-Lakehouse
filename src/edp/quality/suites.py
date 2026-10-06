"""Pre-configured Data Quality check suites."""

from __future__ import annotations

from edp.quality.rules import NullCheck, RangeCheck, SetCheck, UniqueCheck
from edp.quality.runner import DataQualitySuite


def get_silver_suites() -> dict[str, DataQualitySuite]:
    """Returns standard quality suites for Silver tables."""
    return {
        "customers": DataQualitySuite(
            dataset="customers",
            layer="silver",
            rules=(
                NullCheck("customer_id"),
                NullCheck("email"),
                UniqueCheck(["customer_id"]),
            ),
        ),
        "products": DataQualitySuite(
            dataset="products",
            layer="silver",
            rules=(
                NullCheck("product_id"),
                UniqueCheck(["product_id"]),
                RangeCheck("unit_price", min_val=0),
                RangeCheck("unit_cost", min_val=0),
            ),
        ),
        "orders": DataQualitySuite(
            dataset="orders",
            layer="silver",
            rules=(
                NullCheck("order_id"),
                NullCheck("customer_id"),
                UniqueCheck(["order_id"]),
                RangeCheck("total_amount", min_val=0),
                SetCheck(
                    "status", ("pending", "paid", "shipped", "delivered", "cancelled", "returned")
                ),
            ),
        ),
        "order_items": DataQualitySuite(
            dataset="order_items",
            layer="silver",
            rules=(
                NullCheck("order_item_id"),
                UniqueCheck(["order_item_id"]),
                RangeCheck("quantity", min_val=1),
                RangeCheck("unit_price", min_val=0),
            ),
        ),
        "payments": DataQualitySuite(
            dataset="payments",
            layer="silver",
            rules=(
                NullCheck("payment_id"),
                UniqueCheck(["payment_id"]),
                RangeCheck("amount", min_val=0),
                SetCheck("payment_status", ("pending", "completed", "failed", "refunded")),
            ),
        ),
    }


def get_gold_suites() -> dict[str, DataQualitySuite]:
    """Returns standard quality suites for Gold tables."""
    return {
        "fact_sales": DataQualitySuite(
            dataset="fact_sales",
            layer="gold",
            rules=(
                NullCheck("sales_fact_key"),
                UniqueCheck(["sales_fact_key"]),
                RangeCheck("line_total_usd", min_val=0),
            ),
        ),
        "dim_customer": DataQualitySuite(
            dataset="dim_customer",
            layer="gold",
            rules=(
                NullCheck("customer_id"),
                UniqueCheck(["customer_id"]),
            ),
        ),
        "dim_product": DataQualitySuite(
            dataset="dim_product",
            layer="gold",
            rules=(
                NullCheck("product_id"),
                UniqueCheck(["product_id"]),
            ),
        ),
        "dim_date": DataQualitySuite(
            dataset="dim_date",
            layer="gold",
            rules=(
                NullCheck("date_key"),
                UniqueCheck(["date_key"]),
            ),
        ),
    }
