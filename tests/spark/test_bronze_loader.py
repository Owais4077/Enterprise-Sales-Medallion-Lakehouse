import json
from datetime import UTC

import pytest

from edp.common.config import load_yaml_config
from edp.common.paths import Layer
from edp.ingestion.config import IngestionConfig
from edp.ingestion.state import SqliteStateStore
from edp.transformations.bronze.loader import (
    METADATA_COLUMNS,
    BronzeLoader,
    BronzeLoadError,
    DatasetSpec,
    dataset_specs,
    discover_batches,
)
from tests.spark.conftest import EXTRACTED_AT, land_files, land_rows

ORDERS = DatasetSpec("postgres", "orders", "jsonl")
REVIEWS = DatasetSpec("files", "reviews", "json")
COUNTRIES = DatasetSpec("files", "countries", "csv")


@pytest.fixture
def loader(spark, layout, landing):
    return BronzeLoader(spark, layout, landing)


def bronze(spark, layout, dataset):
    return spark.read.format("delta").load(layout.table_path(Layer.BRONZE, dataset))


# ---------------------------------------------------------------- source values stay as text


def test_values_are_kept_as_text_and_lineage_is_added(spark, layout, landing, loader):
    source_row = {
        "order_id": 1,
        "total": "19.90",
        "at": "2025-01-01T00:00:00+00:00",
        "ok": True,
        "n": None,
    }
    land_rows(landing, "postgres", "orders", "b1", [source_row])
    results = loader.load_dataset(ORDERS)

    assert [(r.batch_id, r.rows_loaded) for r in results] == [("b1", 1)]
    df = bronze(spark, layout, "orders")
    row = df.first().asDict()
    assert {
        f.dataType.simpleString()
        for f in df.schema.fields
        if f.name not in ("ingestion_timestamp",)
    } == {"string"}
    assert (row["order_id"], row["total"], row["ok"], row["n"]) == ("1", "19.90", "true", None)
    assert row["batch_id"] == "b1" and row["source_system"] == "postgres"
    assert row["file_name"] is None and row["run_id"] == "run-b1"
    assert row["ingestion_timestamp"].replace(tzinfo=UTC) == EXTRACTED_AT
    assert set(METADATA_COLUMNS) <= set(df.columns)


def test_table_is_delta_partitioned_by_batch(spark, layout, landing, loader):
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}])
    loader.load_dataset(ORDERS)
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{loader.table_path('orders')}`").first()
    assert detail["format"] == "delta" and list(detail["partitionColumns"]) == ["batch_id"]


def test_reloading_is_idempotent(spark, layout, landing, loader):
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": i} for i in range(5)])
    assert len(loader.load_dataset(ORDERS)) == 1
    assert loader.load_dataset(ORDERS) == []  # nothing pending
    assert bronze(spark, layout, "orders").count() == 5


def test_new_batches_append_oldest_first(spark, layout, landing, loader):
    land_rows(landing, "postgres", "orders", "20260101-a", [{"order_id": 1}])
    loader.load_dataset(ORDERS)
    land_rows(landing, "postgres", "orders", "20260102-b", [{"order_id": 2}, {"order_id": 3}])
    results = loader.load_dataset(ORDERS)
    assert [r.batch_id for r in results] == ["20260102-b"]  # only the new batch
    assert bronze(spark, layout, "orders").count() == 3
    assert loader.loaded_batch_ids("orders") == {"20260101-a", "20260102-b"}


# ---------------------------------------------------------------------- schema drift


def test_new_source_columns_are_added_without_touching_old_rows(spark, layout, landing, loader):
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}])
    loader.load_dataset(ORDERS)
    land_rows(landing, "postgres", "orders", "b2", [{"order_id": 2, "coupon": "SAVE10"}])
    loader.load_dataset(ORDERS)
    df = bronze(spark, layout, "orders")
    assert "coupon" in df.columns
    by_id = {r["order_id"]: r["coupon"] for r in df.collect()}
    assert by_id == {"1": None, "2": "SAVE10"}


def test_all_null_column_in_one_batch_does_not_break_later_batches(spark, layout, landing, loader):
    land_rows(landing, "postgres", "customers", "b1", [{"id": 1, "city": None}])
    land_rows(landing, "postgres", "customers", "b2", [{"id": 2, "city": "Paris"}])
    spec = DatasetSpec("postgres", "customers", "jsonl")
    loader.load_dataset(spec)
    assert {r["city"] for r in bronze(spark, layout, "customers").collect()} == {None, "Paris"}


# ---------------------------------------------------------------------- file sources


def test_json_files_keep_bad_values_and_record_file_name(spark, layout, landing, loader):
    good = json.dumps([{"review_id": "r1", "rating": 5}, {"review_id": "r2", "rating": "abc"}])
    other = json.dumps([{"review_id": "r3", "rating": 6}])
    land_files(landing, "files", "reviews", "b1", {
        "reviews_2025-01.json": (good.encode(), 2),
        "reviews_2025-02.json": (other.encode(), 1),
    })  # fmt: skip
    loader.load_dataset(REVIEWS)
    rows = {r["review_id"]: r for r in bronze(spark, layout, "reviews").collect()}
    assert rows["r2"]["rating"] == "abc" and rows["r3"]["rating"] == "6"  # garbage preserved
    assert rows["r1"]["file_name"] == "reviews_2025-01.json"
    assert rows["r3"]["file_name"] == "reviews_2025-02.json"


def test_csv_quoting_and_leading_zeros_survive(spark, layout, landing, loader):
    content = b'code,name,zip\nUS,"United, States",007\nDE,"Ger\nmany",\n'
    land_files(landing, "files", "countries", "b1", {"countries.csv": (content, 2)})
    loader.load_dataset(COUNTRIES)
    rows = {r["code"]: r for r in bronze(spark, layout, "countries").collect()}
    assert rows["US"]["name"] == "United, States" and rows["US"]["zip"] == "007"
    assert rows["DE"]["name"] == "Ger\nmany" and rows["DE"]["zip"] is None
    assert rows["US"]["file_name"] == "countries.csv"


def test_empty_json_array_batch_is_skipped_without_error(spark, layout, landing, loader):
    land_files(landing, "files", "reviews", "b1", {"reviews_2025-01.json": (b"[]", 0)})
    assert loader.load_dataset(REVIEWS) == []
    assert not loader.table_exists("reviews")


# ------------------------------------------------------------------ refusing bad input


def test_row_count_mismatch_with_manifest_writes_nothing(spark, layout, landing, loader):
    batch_dir = land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}, {"order_id": 2}])
    manifest_path = batch_dir / "_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["record_count"] = 3
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(BronzeLoadError, match="manifest says 3"):
        loader.load_dataset(ORDERS)
    assert not loader.table_exists("orders")


def test_tampered_landing_file_is_detected_by_checksum(spark, layout, landing, loader):
    batch_dir = land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}])
    with (batch_dir / "part-00000.jsonl.gz").open("ab") as handle:
        handle.write(b"corruption")
    with pytest.raises(BronzeLoadError, match="checksum"):
        loader.load_dataset(ORDERS)
    assert not loader.table_exists("orders")


def test_missing_landed_file_is_detected(spark, layout, landing, loader):
    batch_dir = land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}])
    (batch_dir / "part-00000.jsonl.gz").unlink()
    with pytest.raises(BronzeLoadError, match="missing"):
        loader.load_dataset(ORDERS)


def test_source_column_clashing_with_metadata_is_refused(spark, layout, landing, loader):
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1, "batch_id": "sneaky"}])
    with pytest.raises(BronzeLoadError, match="collide"):
        loader.load_dataset(ORDERS)


def test_failing_batch_stops_the_dataset_so_order_is_preserved(spark, layout, landing, loader):
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}])
    bad = land_rows(landing, "postgres", "orders", "b2", [{"order_id": 2}])
    land_rows(landing, "postgres", "orders", "b3", [{"order_id": 3}])
    (bad / "part-00000.jsonl.gz").write_bytes(b"not gzip")

    with pytest.raises(BronzeLoadError):
        loader.load_dataset(ORDERS)
    assert loader.loaded_batch_ids("orders") == {"b1"}  # b3 was NOT loaded ahead of b2


def test_in_progress_and_manifestless_batches_are_ignored(landing, tmp_path):
    land_rows(landing, "postgres", "orders", "done", [{"order_id": 1}])
    folder = landing / "postgres" / "orders"
    (folder / ".tmp-batch_id=inflight").mkdir()
    (folder / "batch_id=crashed").mkdir()  # no manifest: never fully published
    assert [b.batch_id for b in discover_batches(landing, ORDERS)] == ["done"]


# ------------------------------------------------------------------------- audit trail


def test_each_batch_load_is_audited(spark, layout, landing, tmp_path):
    state = SqliteStateStore(tmp_path / "state.db")
    loader = BronzeLoader(spark, layout, landing, state=state)
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}, {"order_id": 2}])
    bad = land_rows(landing, "postgres", "orders", "b2", [{"order_id": 3}])
    (bad / "part-00000.jsonl.gz").write_bytes(b"x")

    with pytest.raises(BronzeLoadError):
        loader.load_dataset(ORDERS)
    runs = {r["batch_id"]: r for r in state.list_runs()}
    assert runs["b1"]["status"] == "success" and runs["b1"]["records_processed"] == 2
    assert runs["b1"]["pipeline_name"] == "bronze_load"
    assert runs["b2"]["status"] == "failed" and runs["b2"]["error_type"] == "BronzeLoadError"


# ------------------------------------------------------------------------------ specs


def test_dataset_specs_follow_the_ingestion_config():
    specs = dataset_specs(IngestionConfig.from_dict(load_yaml_config("pipeline")))
    assert specs["orders"] == DatasetSpec("postgres", "orders", "jsonl")
    assert specs["exchange_rates"] == DatasetSpec("rest_api", "exchange_rates", "jsonl")
    assert specs["countries"].kind == "csv" and specs["reviews"].kind == "json"
    assert len(specs) == 8
