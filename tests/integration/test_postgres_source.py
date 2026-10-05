"""Run with: docker compose run --rm app pytest -m integration

Schema tests need only the database; data tests need `generate postgres` to have run.
"""

import json
from pathlib import Path

import pytest
from psycopg2 import errors

from edp.common.config import PROJECT_ROOT

pytestmark = pytest.mark.integration

MANIFEST = PROJECT_ROOT / "data" / "generation_manifest.json"


def scalar(cur, query, params=None):
    cur.execute(query, params)
    return cur.fetchone()[0]


def insert_customer(cur, email="t@example.org"):
    cur.execute(
        "INSERT INTO sales.customers (first_name, last_name, email, country_code, signup_date) "
        "VALUES ('T', 'T', %s, 'US', '2024-01-01') RETURNING customer_id",
        (email,),
    )
    return cur.fetchone()[0]


# --------------------------------------------------------------------- schema behaviour


def test_all_tables_exist(cur):
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'sales'")
    assert {r[0] for r in cur.fetchall()} == {
        "customers", "products", "orders", "order_items", "payments",
    }  # fmt: skip


def test_orphan_order_is_rejected(cur):
    with pytest.raises(errors.ForeignKeyViolation):
        cur.execute(
            "INSERT INTO sales.orders (customer_id, order_date, status, currency, "
            "shipping_country, total_amount) VALUES (-1, now(), 'paid', 'USD', 'US', 10)"
        )


def test_invalid_status_is_rejected(cur):
    customer_id = insert_customer(cur)
    with pytest.raises(errors.CheckViolation):
        cur.execute(
            "INSERT INTO sales.orders (customer_id, order_date, status, currency, "
            "shipping_country, total_amount) VALUES (%s, now(), 'bogus', 'USD', 'US', 10)",
            (customer_id,),
        )


def test_duplicate_email_is_rejected(cur):
    insert_customer(cur, "same@example.org")
    with pytest.raises(errors.UniqueViolation):
        insert_customer(cur, "same@example.org")


def test_update_trigger_bumps_updated_at_but_insert_keeps_supplied_value(cur):
    cur.execute(
        "INSERT INTO sales.customers (first_name, last_name, email, country_code, signup_date, "
        "created_at, updated_at) VALUES ('T','T','hist@example.org','US','2020-01-01', "
        "'2020-01-01', '2020-01-01') RETURNING customer_id, updated_at"
    )
    customer_id, inserted_updated_at = cur.fetchone()
    assert inserted_updated_at.year == 2020  # historical load is not overwritten
    cur.execute("UPDATE sales.customers SET city = 'Paris' WHERE customer_id = %s", (customer_id,))
    cur.execute("SELECT updated_at FROM sales.customers WHERE customer_id = %s", (customer_id,))
    assert cur.fetchone()[0].year >= 2025


# ------------------------------------------------------------------ loaded data checks


@pytest.fixture
def manifest():
    if not MANIFEST.is_file():
        pytest.skip("Run `python -m data_generation.generate all` first")
    return json.loads(Path(MANIFEST).read_text())


def test_row_counts_match_manifest(cur, manifest):
    for table, expected in manifest["row_counts"].items():
        if table in ("countries", "exchange_rates", "reviews"):
            continue  # not stored in PostgreSQL
        actual = scalar(cur, f"SELECT count(*) FROM sales.{table}")  # noqa: S608
        assert actual == expected, table


def test_no_orphans_after_load(cur, manifest):
    assert scalar(cur, "SELECT count(*) FROM sales.order_items i LEFT JOIN sales.orders o "
                       "USING (order_id) WHERE o.order_id IS NULL") == 0  # fmt: skip
    assert scalar(cur, "SELECT count(*) FROM sales.payments p LEFT JOIN sales.orders o "
                       "USING (order_id) WHERE o.order_id IS NULL") == 0  # fmt: skip


def test_injected_defects_are_present_in_database(cur, manifest):
    defects = manifest["injected_defects"]
    assert scalar(cur, "SELECT count(*) FROM sales.orders WHERE total_amount < 0") == (
        defects["order_negative_total"]
    )
    assert scalar(cur, "SELECT count(*) FROM sales.payments WHERE amount < 0") == (
        defects["payment_negative"]
    )
    assert scalar(cur, "SELECT count(*) FROM sales.orders WHERE order_date > created_at") == (
        defects["order_future_date"]
    )
    assert (
        scalar(
            cur,
            "SELECT coalesce(sum(n - 1), 0) FROM "
            "(SELECT count(*) n FROM sales.customers GROUP BY lower(email) HAVING count(*) > 1) d",
        )
        == defects["customer_duplicate"]
    )


def test_identity_sequences_continue_after_bulk_load(cur, manifest):
    # Sequences are not rolled back with transactions, so earlier tests may have consumed ids;
    # the property that matters is that the next id never collides with a loaded row.
    customer_id = insert_customer(cur, "next@example.org")
    assert customer_id > manifest["row_counts"]["customers"]
