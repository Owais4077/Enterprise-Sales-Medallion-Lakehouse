import gzip
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from edp.ingestion.landing import LandingBatch, make_batch_id, sha256_file
from edp.ingestion.models import RunStatus, SourceFileRecord
from edp.ingestion.state import SqliteStateStore

NOW = datetime(2026, 1, 5, 10, 15, tzinfo=UTC)


def read_part(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


# ------------------------------------------------------------------------ landing batch


def test_commit_publishes_atomically_with_manifest(tmp_path):
    batch = LandingBatch(tmp_path, "postgres", "orders", "b1")
    batch.write_rows([{"id": 1}, {"id": 2}])
    assert not (tmp_path / "postgres/orders/batch_id=b1").exists()  # invisible until commit
    final = batch.commit({"watermark_to": "w"})

    manifest = json.loads((final / "_manifest.json").read_text())
    assert manifest["record_count"] == 2 and manifest["watermark_to"] == "w"
    assert read_part(final / manifest["data_files"][0]["name"]) == [{"id": 1}, {"id": 2}]
    assert not list((tmp_path / "postgres/orders").glob(".tmp-*"))  # no temp leftovers


def test_values_are_encoded_losslessly(tmp_path):
    batch = LandingBatch(tmp_path, "s", "d", "b")
    batch.write_rows(
        [
            {
                "price": Decimal("19.90"),
                "at": datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
                "day": date(2025, 1, 2),
                "uid": UUID(int=1),
                "text": "Zoë 東京",
                "none": None,
            }
        ]
    )
    final = batch.commit({})
    row = read_part(final / "part-00000.jsonl.gz")[0]
    assert row["price"] == "19.90"  # string, so trailing zero and precision survive
    assert row["at"] == "2025-01-02T03:04:05+00:00" and row["day"] == "2025-01-02"
    assert row["text"] == "Zoë 東京" and row["none"] is None


def test_unserialisable_values_fail_loudly(tmp_path):
    batch = LandingBatch(tmp_path, "s", "d", "b")
    with pytest.raises(TypeError):
        batch.write_rows([{"x": object()}])
    with pytest.raises(ValueError):
        batch.write_rows([{"x": float("nan")}])
    batch.abort()


def test_part_files_rotate_and_manifest_checksums_match(tmp_path):
    batch = LandingBatch(tmp_path, "s", "d", "b", max_rows_per_file=10)
    batch.write_rows({"n": i} for i in range(25))
    final = batch.commit({})
    parts = json.loads((final / "_manifest.json").read_text())["data_files"]
    assert [p["rows"] for p in parts] == [10, 10, 5]
    assert all(sha256_file(final / p["name"]) == p["sha256"] for p in parts)
    assert sum(len(read_part(final / p["name"])) for p in parts) == 25


def test_abort_removes_everything_and_is_idempotent(tmp_path):
    batch = LandingBatch(tmp_path, "s", "d", "b")
    batch.write_rows([{"a": 1}])
    batch.abort()
    batch.abort()
    assert list((tmp_path / "s/d").iterdir()) == []


def test_empty_batch_is_detected(tmp_path):
    batch = LandingBatch(tmp_path, "s", "d", "b")
    assert batch.is_empty
    batch.abort()


def test_source_files_are_copied_byte_for_byte(tmp_path):
    source = tmp_path / "in.csv"
    source.write_bytes(b"a,b\r\n1,\xc3\xa9\r\n")  # CRLF and non-ASCII must survive untouched
    batch = LandingBatch(tmp_path / "landing", "files", "countries", "b")
    batch.add_file(source, record_count=1, sha256=sha256_file(source))
    final = batch.commit({})
    assert (final / "in.csv").read_bytes() == source.read_bytes()
    manifest = json.loads((final / "_manifest.json").read_text())
    assert manifest["source_files"][0]["sha256"] == sha256_file(source)
    assert manifest["record_count"] == 1


def test_duplicate_batch_id_is_refused(tmp_path):
    LandingBatch(tmp_path, "s", "d", "b").commit({})
    with pytest.raises(FileExistsError):
        LandingBatch(tmp_path, "s", "d", "b")


def test_batch_ids_are_unique_and_sortable():
    first, second = make_batch_id(NOW), make_batch_id(NOW)
    assert first != second and first.startswith("20260105T101500Z-")


# ---------------------------------------------------------------------------- state store


@pytest.fixture
def store(tmp_path):
    return SqliteStateStore(tmp_path / "state" / "s.db")


def test_watermark_roundtrip_and_upsert(store):
    assert store.get_watermark("postgres", "orders") is None
    for value in ("2025-01-01T00:00:00+00:00", "2025-02-01T00:00:00+00:00"):
        store.commit_extraction(
            source_system="postgres",
            dataset="orders",
            run_id="r",
            batch_id="b",
            watermark=value,
            files=[],
            now=NOW,
        )
    assert store.get_watermark("postgres", "orders") == "2025-02-01T00:00:00+00:00"
    assert store.get_watermark("postgres", "customers") is None  # independent per dataset


def test_none_watermark_leaves_existing_value(store):
    kwargs = {
        "source_system": "s",
        "dataset": "d",
        "run_id": "r",
        "batch_id": "b",
        "files": [],
        "now": NOW,
    }
    store.commit_extraction(watermark="w1", **kwargs)
    store.commit_extraction(watermark=None, **kwargs)
    assert store.get_watermark("s", "d") == "w1"


def test_processed_files_keyed_by_name_and_hash(store):
    record = SourceFileRecord("a.json", "hash1", 3, 10)
    store.commit_extraction(
        source_system="files",
        dataset="reviews",
        run_id="r",
        batch_id="b",
        watermark=None,
        files=[record],
        now=NOW,
    )
    assert store.is_file_processed("files", "reviews", "a.json", "hash1")
    assert not store.is_file_processed("files", "reviews", "a.json", "other-hash")  # changed
    assert not store.is_file_processed("files", "reviews", "b.json", "hash1")


def test_state_survives_a_new_store_instance(tmp_path):
    path = tmp_path / "s.db"
    SqliteStateStore(path).commit_extraction(
        source_system="s", dataset="d", run_id="r", batch_id="b", watermark="w", files=[], now=NOW
    )
    assert SqliteStateStore(path).get_watermark("s", "d") == "w"


def test_audit_lifecycle_and_error_capture(store):
    common = {
        "pipeline_name": "p",
        "source_system": "s",
        "dataset": "d",
        "started_at": NOW,
        "watermark_from": None,
    }
    store.start_run(run_id="ok", batch_id="b1", **common)
    store.start_run(run_id="bad", batch_id="b2", **common)
    assert {r["status"] for r in store.list_runs()} == {"running"}

    store.finish_run(
        run_id="ok",
        status=RunStatus.SUCCESS,
        finished_at=NOW,
        records_extracted=5,
        records_processed=5,
        records_rejected=0,
        watermark_to="w",
    )
    store.finish_run(
        run_id="bad",
        status=RunStatus.FAILED,
        finished_at=NOW,
        records_extracted=2,
        records_processed=0,
        records_rejected=0,
        watermark_to=None,
        error=ConnectionError("db down"),
    )
    runs = {r["run_id"]: r for r in store.list_runs()}
    assert runs["ok"]["status"] == "success" and runs["ok"]["records_processed"] == 5
    assert runs["bad"]["error_type"] == "ConnectionError"
    assert runs["bad"]["error_message"] == "db down"
    assert runs["bad"]["finished_at"] is not None


def test_long_error_messages_are_truncated(store):
    store.start_run(
        run_id="r",
        pipeline_name="p",
        source_system="s",
        dataset="d",
        batch_id="b",
        started_at=NOW,
        watermark_from=None,
    )
    store.finish_run(
        run_id="r",
        status=RunStatus.FAILED,
        finished_at=NOW,
        records_extracted=0,
        records_processed=0,
        records_rejected=0,
        watermark_to=None,
        error=RuntimeError("x" * 10_000),
    )
    assert len(store.list_runs()[0]["error_message"]) == 2000
