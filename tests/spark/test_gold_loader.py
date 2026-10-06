from datetime import UTC, datetime

import pytest

from edp.common.paths import Layer
from edp.transformations.gold.dimensions import build_dim_date
from edp.transformations.gold.loader import GoldLoader


@pytest.fixture
def gold_loader(spark, layout):
    return GoldLoader(spark, layout)


def write_silver_table(spark, layout, dataset, rows):
    path = layout.table_path(Layer.SILVER, dataset)
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("overwrite").save(path)


def gold(spark, layout, table_name):
    return spark.read.format("delta").load(layout.table_path(Layer.GOLD, table_name))


def test_build_dim_date(spark):
    df = build_dim_date(spark, "2025-01-01", "2025-01-05")
    assert df.count() == 5
    first = df.filter(df.date_key == 20250101).first()
    assert first["year"] == 2025
    assert first["month"] == 1
    assert first["month_name"] == "January"
    assert first["day"] == 1


def test_gold_loader_builds_star_schema_and_facts(spark, layout, gold_loader):
    # 1. Silver Customers
    cust_rows = [
        {
            "customer_id": 1,
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "phone": "555-1234",
            "country_code": "US",
            "city": "Dallas",
            "segment": "consumer",
            "signup_date": datetime(2025, 1, 1).date(),
            "is_active": True,
        }
    ]
    write_silver_table(spark, layout, "customers", cust_rows)

    # 2. Silver Products
    prod_rows = [
        {
            "product_id": 10,
            "sku": "SKU-10",
            "product_name": "Widget A",
            "category": "Electronics",
            "unit_price": 100.00,
            "unit_cost": 60.00,
            "currency": "USD",
            "is_active": True,
        }
    ]
    write_silver_table(spark, layout, "products", prod_rows)

    # 3. Silver Orders
    order_rows = [
        {
            "order_id": 1000,
            "customer_id": 1,
            "order_date": datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
            "status": "delivered",
            "currency": "EUR",
            "shipping_country": "DE",
            "total_amount": 200.00,
        }
    ]
    write_silver_table(spark, layout, "orders", order_rows)

    # 4. Silver Order Items
    item_rows = [
        {
            "order_item_id": 50,
            "order_id": 1000,
            "product_id": 10,
            "quantity": 2,
            "unit_price": 100.00,
            "discount_pct": 10.00,  # 2 * 100 * (1 - 0.10) = 180.00 EUR
        }
    ]
    write_silver_table(spark, layout, "order_items", item_rows)

    # 5. Silver Exchange Rates (EUR to USD rate = 1.10)
    rate_rows = [
        {
            "rate_date": datetime(2025, 1, 15).date(),
            "quote_currency": "EUR",
            "rate_to_usd": 1.10,
        }
    ]
    write_silver_table(spark, layout, "exchange_rates", rate_rows)

    # Build Gold Tables
    results = gold_loader.build_gold_tables()
    result_names = {r.table_name for r in results}

    assert "dim_date" in result_names
    assert "dim_customer" in result_names
    assert "dim_product" in result_names
    assert "fact_sales" in result_names
    assert "kpi_monthly_sales" in result_names

    # Assert Fact Sales Calculations
    df_facts = gold(spark, layout, "fact_sales")
    assert df_facts.count() == 1
    fact_row = df_facts.first()

    assert fact_row["sales_fact_key"] == "1000_50"
    assert fact_row["order_date_key"] == 20250115
    assert float(fact_row["line_total_local"]) == 180.00
    assert float(fact_row["exchange_rate_to_usd"]) == 1.10
    assert float(fact_row["line_total_usd"]) == 198.00  # 180.00 * 1.10

    # Assert KPI Aggregations
    df_kpis = gold(spark, layout, "kpi_monthly_sales")
    assert df_kpis.count() == 1
    kpi_row = df_kpis.first()
    assert kpi_row["year"] == 2025
    assert kpi_row["month"] == 1
    assert kpi_row["shipping_country"] == "DE"
    assert kpi_row["total_orders"] == 1
    assert float(kpi_row["total_revenue_usd"]) == 198.00
