import csv
import json
import re
from pathlib import Path

import pytest

from data_generation import file_writers
from data_generation.generators import GenConfig, generate_dataset
from data_generation.pg_loader import ID_COLUMN, TABLE_COLUMNS
from tests.unit.test_generators import RAW_CONFIG

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "postgres"


@pytest.fixture(scope="module")
def cfg_ds():
    cfg = GenConfig.from_dict(RAW_CONFIG)
    return cfg, generate_dataset(cfg)


def test_countries_csv_round_trip(tmp_path, cfg_ds):
    _, ds = cfg_ds
    path = file_writers.write_countries_csv(ds.countries, tmp_path / "countries.csv")
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == len(ds.countries)
    assert list(rows[0]) == list(file_writers.COUNTRY_COLUMNS)


def test_reviews_are_split_into_monthly_files_and_stale_files_removed(tmp_path, cfg_ds):
    _, ds = cfg_ds
    stale = tmp_path / "reviews_1999-01.json"
    stale.write_text("[]")
    paths = file_writers.write_reviews_json(ds.reviews, tmp_path)
    assert not stale.exists()
    assert all(re.fullmatch(r"reviews_\d{4}-\d{2}\.json", p.name) for p in paths)
    loaded = [r for p in paths for r in json.loads(p.read_text(encoding="utf-8"))]
    assert len(loaded) == len(ds.reviews)
    for p in paths:
        month = p.stem.removeprefix("reviews_")
        assert all(r["review_date"].startswith(month) for r in json.loads(p.read_text("utf-8")))


def test_exchange_rate_seed_file_is_json_serialisable(tmp_path, cfg_ds):
    _, ds = cfg_ds
    path = file_writers.write_exchange_rates_json(ds.exchange_rates, tmp_path / "fx.json")
    rows = json.loads(path.read_text())
    assert len(rows) == len(ds.exchange_rates)
    assert {"rate_date", "base_currency", "quote_currency", "rate", "updated_at"} == set(rows[0])


def test_manifest_matches_dataset(cfg_ds):
    cfg, ds = cfg_ds
    manifest = file_writers.build_manifest(cfg, ds)
    assert manifest["row_counts"]["orders"] == len(ds.orders)
    assert manifest["injected_defects"] == dict(sorted(ds.defects.items()))
    json.dumps(manifest)  # must be serialisable


# --- SQL scripts: cheap static checks that run without a database ---------------------


def sql_text() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in sorted(SQL_DIR.glob("*.sql")))


def test_sql_scripts_are_numbered_in_order():
    names = [p.name for p in sorted(SQL_DIR.glob("*.sql"))]
    assert names == ["001_schema.sql", "002_indexes.sql", "003_triggers.sql"]


def table_blocks() -> dict[str, str]:
    pattern = re.compile(r"CREATE TABLE sales\.(\w+) \((.*?)\n\);", re.S)
    return {m.group(1): m.group(2) for m in pattern.finditer(sql_text())}


def test_every_table_has_pk_timestamps_and_update_trigger():
    blocks = table_blocks()
    assert set(blocks) == set(TABLE_COLUMNS)
    for table, body in blocks.items():
        assert "PRIMARY KEY" in body, table
        assert "created_at" in body and "updated_at" in body, table
        assert re.search(rf"BEFORE UPDATE ON sales\.{table}\b", sql_text()), table
        assert re.search(rf"ON sales\.{table} \(updated_at\)", sql_text()), table


def test_loader_columns_exist_in_ddl():
    for table, body in table_blocks().items():
        for column in TABLE_COLUMNS[table]:
            assert re.search(rf"^\s*{column}\s", body, re.M), f"{table}.{column} missing from DDL"
        assert ID_COLUMN[table] in TABLE_COLUMNS[table]


def test_foreign_key_columns_are_indexed():
    text = sql_text()
    for table, column in (
        ("orders", "customer_id"),
        ("order_items", "order_id"),
        ("order_items", "product_id"),
        ("payments", "order_id"),
    ):
        assert re.search(rf"ON sales\.{table} \({column}\)", text), f"{table}.{column}"
