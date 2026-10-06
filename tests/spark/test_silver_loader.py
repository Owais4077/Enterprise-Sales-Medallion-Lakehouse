import pytest

from edp.common.paths import Layer
from edp.transformations.bronze.loader import BronzeLoader, DatasetSpec
from edp.transformations.silver.loader import SilverLoader
from tests.spark.conftest import land_rows

CUSTOMERS_SPEC = DatasetSpec("postgres", "customers", "jsonl")
ORDERS_SPEC = DatasetSpec("postgres", "orders", "jsonl")


@pytest.fixture
def silver_loader(spark, layout):
    return SilverLoader(spark, layout)


def silver(spark, layout, dataset):
    return spark.read.format("delta").load(layout.table_path(Layer.SILVER, dataset))


def quarantine(spark, layout, dataset):
    q_path = f"{layout.table_path(Layer.SILVER, '_quarantine')}/{dataset}"
    return spark.read.format("delta").load(q_path)


def test_silver_loader_casts_and_merges_valid_rows(spark, layout, landing, silver_loader):
    rows = [
        {
            "customer_id": 1,
            "first_name": "Alice",
            "last_name": "Smith",
            "email": "alice@example.com",
            "phone": "123456",
            "country_code": "US",
            "city": "New York",
            "segment": "consumer",
            "signup_date": "2025-01-01",
            "is_active": True,
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
        }
    ]
    land_rows(landing, "postgres", "customers", "b1", rows)
    BronzeLoader(spark, layout, landing).load_dataset(CUSTOMERS_SPEC)

    res = silver_loader.load_dataset("customers")
    assert res.rows_processed == 1
    assert res.rows_valid == 1
    assert res.rows_quarantined == 0

    df = silver(spark, layout, "customers")
    assert df.count() == 1
    row = df.first()
    assert row["customer_id"] == 1
    assert row["first_name"] == "Alice"
    assert row["email"] == "alice@example.com"


def test_silver_loader_routes_bad_rows_to_quarantine(spark, layout, landing, silver_loader):
    rows = [
        {
            "customer_id": 2,
            "first_name": "Bob",
            "last_name": "Jones",
            "email": "invalid-email-format",
            "phone": "123456",
            "country_code": "US",
            "city": "Boston",
            "segment": "consumer",
            "signup_date": "2025-01-01",
            "is_active": True,
            "created_at": "2025-01-01T00:00:00+00:00",
            "updated_at": "2025-01-01T00:00:00+00:00",
        }
    ]
    land_rows(landing, "postgres", "customers", "b1", rows)
    BronzeLoader(spark, layout, landing).load_dataset(CUSTOMERS_SPEC)

    res = silver_loader.load_dataset("customers")
    assert res.rows_processed == 1
    assert res.rows_valid == 0
    assert res.rows_quarantined == 1

    q_df = quarantine(spark, layout, "customers")
    assert q_df.count() == 1
    q_row = q_df.first()
    assert "Invalid email format" in q_row["quarantine_reason"]


def test_silver_loader_merge_updates_existing_records(spark, layout, landing, silver_loader):
    # Batch 1
    rows_b1 = [
        {
            "order_id": 100,
            "customer_id": 1,
            "order_date": "2025-01-01T10:00:00+00:00",
            "status": "pending",
            "currency": "USD",
            "shipping_country": "US",
            "total_amount": "50.00",
            "created_at": "2025-01-01T10:00:00+00:00",
            "updated_at": "2025-01-01T10:00:00+00:00",
        }
    ]
    land_rows(landing, "postgres", "orders", "b1", rows_b1)
    BronzeLoader(spark, layout, landing).load_dataset(ORDERS_SPEC)
    silver_loader.load_dataset("orders")

    assert silver(spark, layout, "orders").first()["status"] == "pending"

    # Batch 2 - status updated to 'paid'
    rows_b2 = [
        {
            "order_id": 100,
            "customer_id": 1,
            "order_date": "2025-01-01T10:00:00+00:00",
            "status": "paid",
            "currency": "USD",
            "shipping_country": "US",
            "total_amount": "50.00",
            "created_at": "2025-01-01T10:00:00+00:00",
            "updated_at": "2025-01-01T12:00:00+00:00",  # newer updated_at
        }
    ]
    land_rows(landing, "postgres", "orders", "b2", rows_b2)
    BronzeLoader(spark, layout, landing).load_dataset(ORDERS_SPEC)
    silver_loader.load_dataset("orders")

    df_orders = silver(spark, layout, "orders")
    assert df_orders.count() == 1
    assert df_orders.first()["status"] == "paid"
