"""Bulk-load the generated dataset into the source PostgreSQL database."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2 import sql

from data_generation.generators import Dataset, Row
from edp.common.config import Settings

logger = logging.getLogger(__name__)

SCHEMA = "sales"

# Parents before children, so foreign keys are satisfied while loading.
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": (
        "customer_id", "first_name", "last_name", "email", "phone", "country_code", "city",
        "segment", "signup_date", "is_active", "created_at", "updated_at",
    ),
    "products": (
        "product_id", "sku", "product_name", "category", "unit_price", "unit_cost", "currency",
        "is_active", "created_at", "updated_at",
    ),
    "orders": (
        "order_id", "customer_id", "order_date", "status", "currency", "shipping_country",
        "total_amount", "created_at", "updated_at",
    ),
    "order_items": (
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct",
        "created_at", "updated_at",
    ),
    "payments": (
        "payment_id", "order_id", "payment_method", "payment_status", "amount", "currency",
        "paid_at", "created_at", "updated_at",
    ),
}  # fmt: skip
ID_COLUMN = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}


def connect(settings: Settings) -> Any:
    """Open a connection using environment-provided credentials."""
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=10,
    )


def _format(value: Any) -> str | None:
    if value is None:
        return None  # csv.writer renders None as an empty field, which COPY reads as NULL
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _to_csv(rows: list[Row], columns: tuple[str, ...]) -> io.StringIO:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for row in rows:
        writer.writerow([_format(row[column]) for column in columns])
    buffer.seek(0)
    return buffer


def load_dataset(conn: Any, dataset: Dataset) -> dict[str, int]:
    """Replace the contents of all source tables in ONE transaction.

    Either the whole load succeeds or nothing changes. Returns rows loaded per table.
    """
    tables = {name: getattr(dataset, name) for name in TABLE_COLUMNS}
    loaded: dict[str, int] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
                    sql.SQL(", ").join(sql.Identifier(SCHEMA, t) for t in reversed(tables))
                )
            )
            for table, rows in tables.items():
                columns = TABLE_COLUMNS[table]
                copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')").format(
                    sql.Identifier(SCHEMA, table),
                    sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                )
                cur.copy_expert(copy_sql.as_string(cur), _to_csv(rows, columns))
                loaded[table] = len(rows)
                logger.info("Loaded %s rows into %s.%s", f"{len(rows):,}", SCHEMA, table)
            for table, id_column in ID_COLUMN.items():
                # COPY supplied explicit ids, so move each identity sequence past them.
                cur.execute(
                    sql.SQL(
                        "SELECT setval(pg_get_serial_sequence(%s, %s), "
                        "(SELECT COALESCE(MAX({id}), 1) FROM {tbl}))"
                    ).format(id=sql.Identifier(id_column), tbl=sql.Identifier(SCHEMA, table)),
                    (f"{SCHEMA}.{table}", id_column),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Load failed; transaction rolled back, tables unchanged")
        raise
    return loaded
