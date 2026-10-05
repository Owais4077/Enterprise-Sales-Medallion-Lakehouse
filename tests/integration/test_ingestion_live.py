"""Ingestion against the real PostgreSQL and mock API containers.

    docker compose up -d postgres mock-api
    docker compose run --rm app python -m data_generation.generate all
    docker compose run --rm app pytest -m integration

Incremental-logic tests use a throw-away schema, so the real ``sales`` data is only ever read.
"""

import gzip
import json
import uuid

import httpx
import psycopg2
import pytest

from edp.common.config import PROJECT_ROOT, Settings, get_settings, load_yaml_config
from edp.common.db import connect_postgres
from edp.ingestion import cli
from edp.ingestion.config import ApiDatasetConfig, IngestionConfig, TableConfig
from edp.ingestion.postgres import PostgresExtractor
from edp.ingestion.rest_api import RestApiExtractor
from edp.ingestion.retry import RetryPolicy
from edp.ingestion.runner import IngestionRunner
from edp.ingestion.state import SqliteStateStore

pytestmark = pytest.mark.integration

MANIFEST = PROJECT_ROOT / "data" / "generation_manifest.json"
NO_WAIT = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, jitter=0.0)


def landed_rows(batch_dir):
    rows = []
    for part in sorted(batch_dir.glob("part-*.jsonl.gz")):
        with gzip.open(part, "rt", encoding="utf-8") as handle:
            rows += [json.loads(line) for line in handle]
    return rows


@pytest.fixture
def runner_and_state(tmp_path):
    state = SqliteStateStore(tmp_path / "state.db")
    return IngestionRunner(state=state, landing_root=tmp_path / "landing"), state


@pytest.fixture
def manifest():
    if not MANIFEST.is_file():
        pytest.skip("Run `python -m data_generation.generate all` first")
    return json.loads(MANIFEST.read_text())


# ------------------------------------------------------------- PostgreSQL: incremental logic


@pytest.fixture
def scratch_schema(pg_connection):
    schema = f"edp_it_{uuid.uuid4().hex[:8]}"
    with pg_connection.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")  # noqa: S608
        cur.execute(
            f"CREATE TABLE {schema}.items (id bigint PRIMARY KEY, name text, "  # noqa: S608
            "price numeric(10,2), updated_at timestamptz NOT NULL)"
        )
    pg_connection.commit()
    yield schema
    with pg_connection.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")  # noqa: S608
    pg_connection.commit()


def put(conn, schema, item_id, name, age, price="19.90"):
    """Upsert a row whose updated_at is `age` (a Postgres interval) before the DB's now()."""
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {schema}.items VALUES (%s, %s, %s, now() - %s::interval) "  # noqa: S608
            "ON CONFLICT (id) DO UPDATE SET name = excluded.name, "
            "updated_at = excluded.updated_at",
            (item_id, name, price, age),
        )
    conn.commit()


def extractor(schema, lag=0.0, connect=None):
    return PostgresExtractor(
        connect=connect or (lambda: connect_postgres(Settings())),
        table=TableConfig("items", ("id",), "updated_at"),
        schema=schema,
        batch_size=2,  # tiny, to exercise the multi-fetch loop
        lag_seconds=lag,
        retry=NO_WAIT,
        sleep=lambda s: None,
    )


def test_new_updated_and_unchanged_rows_are_told_apart(
    pg_connection, scratch_schema, runner_and_state
):
    runner, state = runner_and_state
    for i in (1, 2, 3):
        put(pg_connection, scratch_schema, i, f"item{i}", "1 hour")

    first = runner.run(extractor(scratch_schema))
    assert sorted(r["id"] for r in landed_rows(first.landing_path)) == [1, 2, 3]
    assert state.get_watermark("postgres", "items") == first.watermark_to

    idle = runner.run(extractor(scratch_schema))
    assert (
        idle.records_extracted == 0 and idle.landing_path is None
    )  # nothing new -> nothing landed

    put(pg_connection, scratch_schema, 4, "brand new", "0 seconds")  # new
    put(pg_connection, scratch_schema, 2, "renamed", "0 seconds")  # updated
    third = runner.run(extractor(scratch_schema))
    rows = {r["id"]: r for r in landed_rows(third.landing_path)}
    assert sorted(rows) == [2, 4]  # row 1 and 3 (unchanged) were NOT read again
    assert rows[2]["name"] == "renamed"


def test_lag_excludes_rows_too_fresh_to_be_safe_then_picks_them_up(
    pg_connection, scratch_schema, runner_and_state
):
    runner, _ = runner_and_state
    put(pg_connection, scratch_schema, 1, "old", "2 hours")
    put(pg_connection, scratch_schema, 2, "fresh", "5 minutes")

    first = runner.run(extractor(scratch_schema, lag=3600))  # only rows older than 1 hour
    assert [r["id"] for r in landed_rows(first.landing_path)] == [1]

    second = runner.run(extractor(scratch_schema, lag=60))  # fresh row has aged past the lag
    assert [r["id"] for r in landed_rows(second.landing_path)] == [2]


def test_full_refresh_rereads_everything(pg_connection, scratch_schema, runner_and_state):
    runner, _ = runner_and_state
    put(pg_connection, scratch_schema, 1, "a", "1 hour")
    runner.run(extractor(scratch_schema))
    again = runner.run(extractor(scratch_schema), full_refresh=True)
    assert again.records_extracted == 1


def test_values_land_losslessly(pg_connection, scratch_schema, runner_and_state):
    runner, _ = runner_and_state
    put(pg_connection, scratch_schema, 1, "x", "1 hour", price="19.90")
    result = runner.run(extractor(scratch_schema))
    row = landed_rows(result.landing_path)[0]
    assert row["price"] == "19.90"  # Decimal kept exact, not 19.9
    assert row["updated_at"].endswith("+00:00") or "+" in row["updated_at"]


def test_missing_table_fails_run_without_moving_watermark(pg_connection, runner_and_state):
    runner, state = runner_and_state
    with pytest.raises(psycopg2.errors.UndefinedTable):
        runner.run(extractor("no_such_schema"))
    assert state.get_watermark("postgres", "items") is None
    assert state.list_runs()[0]["status"] == "failed"
    assert state.list_runs()[0]["error_type"] == "UndefinedTable"


def test_connection_is_retried_until_the_database_answers(
    pg_connection, scratch_schema, runner_and_state
):
    runner, _ = runner_and_state
    put(pg_connection, scratch_schema, 1, "a", "1 hour")
    attempts = {"n": 0}

    def flaky_connect():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise psycopg2.OperationalError("could not connect (simulated)")
        return connect_postgres(Settings())

    result = runner.run(extractor(scratch_schema, connect=flaky_connect))
    assert attempts["n"] == 3 and result.records_extracted == 1


# --------------------------------------------------------------- real data: read-only checks


def test_real_tables_extract_matches_database_counts(pg_connection, runner_and_state, manifest):
    runner, _ = runner_and_state
    config = IngestionConfig.from_dict(load_yaml_config("pipeline"))
    for name in ("customers", "products"):
        table = config.tables[name]
        result = runner.run(
            PostgresExtractor(
                connect=lambda: connect_postgres(Settings()), table=table, schema="sales",
                batch_size=config.batch_size, lag_seconds=0, retry=NO_WAIT,
            )  # fmt: skip
        )
        with pg_connection.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM sales.{name}")  # noqa: S608
            expected = cur.fetchone()[0]
        pg_connection.rollback()
        assert result.records_extracted == expected == manifest["row_counts"][name]
        assert len({r[table.primary_key[0]] for r in landed_rows(result.landing_path)}) == expected


# ---------------------------------------------------------------------------- live REST API


def test_live_api_full_pull(runner_and_state, manifest):
    settings = Settings()
    client = httpx.Client(base_url=settings.api_base_url, timeout=10)
    try:
        client.get("/health").raise_for_status()
    except httpx.HTTPError as exc:
        pytest.skip(f"mock API not reachable at {settings.api_base_url}: {exc}")
    runner, _ = runner_and_state
    api = ApiDatasetConfig("exchange_rates", "/api/v1/exchange-rates", "updated_at", ("rate_date",))
    result = runner.run(
        RestApiExtractor(
            client=client, api=api, api_key=settings.api_key.get_secret_value(),
            page_size=500, retry=NO_WAIT,
        )  # fmt: skip
    )
    assert result.records_extracted == manifest["row_counts"]["exchange_rates"]


# ----------------------------------------------------------------- the whole CLI, end to end


@pytest.fixture
def cli_env(tmp_path, monkeypatch):
    monkeypatch.setenv("LANDING_DIR", str(tmp_path / "landing"))
    monkeypatch.setenv("STATE_DB_PATH", str(tmp_path / "state.db"))
    get_settings.cache_clear()
    yield tmp_path
    get_settings.cache_clear()


def test_cli_ingests_every_source_then_goes_idle(cli_env, manifest, capsys):
    store = SqliteStateStore(cli_env / "state.db")

    assert cli.main([]) == 0  # first run: everything is new
    first = {(r["source_system"], r["dataset"]): r for r in store.list_runs(100)}
    assert len(first) == 8
    for table in ("customers", "products", "orders", "order_items", "payments"):
        assert first[("postgres", table)]["records_processed"] == manifest["row_counts"][table]
    assert first[("rest_api", "exchange_rates")]["records_processed"] == (
        manifest["row_counts"]["exchange_rates"]
    )
    assert first[("files", "countries")]["records_processed"] == manifest["row_counts"]["countries"]
    assert first[("files", "reviews")]["records_processed"] == manifest["row_counts"]["reviews"]
    assert all(r["status"] == "success" for r in first.values())

    assert cli.main([]) == 0  # second run: incremental
    second = store.list_runs(100)[:8]
    by_key = {(r["source_system"], r["dataset"]): r for r in second}
    for table in ("customers", "products", "orders", "order_items", "payments"):
        assert by_key[("postgres", table)]["records_extracted"] == 0
    assert by_key[("files", "countries")]["records_extracted"] == 0
    assert by_key[("files", "reviews")]["records_extracted"] == 0
    assert 0 < by_key[("rest_api", "exchange_rates")]["records_extracted"] < 20  # boundary overlap
    assert "success" in capsys.readouterr().out


def test_cli_rejects_unknown_dataset(cli_env):
    assert cli.main(["--dataset", "nope"]) == 1
