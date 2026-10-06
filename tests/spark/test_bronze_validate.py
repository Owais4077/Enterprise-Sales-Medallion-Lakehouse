import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from edp.common.paths import Layer
from edp.transformations.bronze.loader import BronzeLoader, DatasetSpec
from edp.transformations.bronze.validate import validate_dataset
from tests.spark.conftest import land_files, land_rows

ORDERS = DatasetSpec("postgres", "orders", "jsonl")
COUNTRIES = DatasetSpec("files", "countries", "csv")


@pytest.fixture
def loaded(spark, layout, landing):
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": i} for i in range(10)])
    land_rows(landing, "postgres", "orders", "b2", [{"order_id": i} for i in range(10, 15)])
    BronzeLoader(spark, layout, landing).load_dataset(ORDERS)
    return layout.table_path(Layer.BRONZE, "orders")


def run(spark, layout, landing, spec=ORDERS):
    return {r.check: r for r in validate_dataset(spark, layout, landing, spec)}


def test_clean_load_passes_every_check(spark, layout, landing, loaded):
    checks = run(spark, layout, landing)
    assert all(c.passed for c in checks.values()), checks
    assert {"table_exists", "all_batches_loaded", "row_counts_match_manifest",
            "lineage_columns_populated", "source_system_consistent"} <= set(checks)  # fmt: skip


def test_unloaded_landed_batch_is_reported(spark, layout, landing, loaded):
    land_rows(landing, "postgres", "orders", "b3", [{"order_id": 99}])
    checks = run(spark, layout, landing)
    assert not checks["all_batches_loaded"].passed and "b3" in checks["all_batches_loaded"].detail


def test_missing_table_is_reported(spark, layout, landing):
    land_rows(landing, "postgres", "orders", "b1", [{"order_id": 1}])
    [check] = validate_dataset(spark, layout, landing, ORDERS)
    assert check.check == "table_exists" and not check.passed


def test_nothing_landed_and_no_table_is_fine(spark, layout, landing):
    [check] = validate_dataset(spark, layout, landing, ORDERS)
    assert check.passed


def test_lost_rows_are_detected(spark, layout, landing, loaded):
    DeltaTable.forPath(spark, loaded).delete("batch_id = 'b1' AND order_id IN ('0', '1')")
    checks = run(spark, layout, landing)
    assert not checks["row_counts_match_manifest"].passed
    assert "b1" in checks["row_counts_match_manifest"].detail


def test_missing_lineage_values_are_detected(spark, layout, landing, loaded):
    broken = spark.read.format("delta").load(loaded).limit(1).withColumn(
        "ingestion_timestamp", F.lit(None).cast("timestamp")
    )  # fmt: skip
    broken.write.format("delta").mode("append").save(loaded)
    checks = run(spark, layout, landing)
    assert not checks["lineage_columns_populated"].passed


def test_wrong_source_system_is_detected(spark, layout, landing, loaded):
    DeltaTable.forPath(spark, loaded).update("batch_id = 'b2'", {"source_system": F.lit("other")})
    assert not run(spark, layout, landing)["source_system_consistent"].passed


def test_file_sources_must_have_file_name(spark, layout, landing):
    land_files(landing, "files", "countries", "b1", {"c.csv": (b"code\nUS\n", 1)})
    BronzeLoader(spark, layout, landing).load_dataset(COUNTRIES)
    assert run(spark, layout, landing, COUNTRIES)["file_name_populated"].passed

    path = layout.table_path(Layer.BRONZE, "countries")
    DeltaTable.forPath(spark, path).update("batch_id = 'b1'", {"file_name": F.lit(None)})
    assert not run(spark, layout, landing, COUNTRIES)["file_name_populated"].passed
