"""``validate_bronze``: cheap structural checks run between the Bronze load and Silver.

This is deliberately *not* the full data-quality framework (Phase 8). It answers one question:
"Is what is in Bronze exactly what was landed?" - every batch present, row counts equal to the
manifests, lineage columns filled in. If not, Silver must not run on top of it.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from edp.common.paths import LakeLayout, Layer
from edp.transformations.bronze.loader import (
    DatasetSpec,
    LandedBatch,
    discover_batches,
)


@dataclass(frozen=True)
class CheckResult:
    dataset: str
    check: str
    passed: bool
    detail: str


def validate_dataset(
    spark: SparkSession, layout: LakeLayout, landing_root: Path, spec: DatasetSpec
) -> list[CheckResult]:
    name = spec.dataset
    expected: list[LandedBatch] = [
        b for b in discover_batches(landing_root, spec) if b.record_count > 0
    ]
    path = layout.table_path(Layer.BRONZE, name)

    if not DeltaTable.isDeltaTable(spark, path):
        if not expected:
            return [CheckResult(name, "table_exists", True, "no landed batches, no table needed")]
        return [
            CheckResult(name, "table_exists", False, f"{len(expected)} landed batch(es), no table")
        ]

    df = spark.read.format("delta").load(path)
    counts = {
        r["batch_id"]: r["n"] for r in df.groupBy("batch_id").agg(F.count("*").alias("n")).collect()
    }
    results = [CheckResult(name, "table_exists", True, path)]

    missing = [b.batch_id for b in expected if b.batch_id not in counts]
    results.append(
        CheckResult(
            name, "all_batches_loaded", not missing,
            f"{len(expected) - len(missing)}/{len(expected)} landed batches present"
            + (f"; missing {missing}" if missing else ""),
        )  # fmt: skip
    )

    wrong = {
        b.batch_id: (counts[b.batch_id], b.record_count)
        for b in expected
        if b.batch_id in counts and counts[b.batch_id] != b.record_count
    }
    results.append(
        CheckResult(
            name, "row_counts_match_manifest", not wrong,
            "bronze == manifest for every batch" if not wrong
            else f"(bronze, manifest) mismatches: {wrong}",
        )  # fmt: skip
    )

    nulls = df.select(
        *[
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in ("ingestion_timestamp", "source_system", "batch_id")
        ]
    ).first()
    null_counts = {c: nulls[c] or 0 for c in nulls.asDict()}
    results.append(
        CheckResult(
            name, "lineage_columns_populated", not any(null_counts.values()),
            "no nulls in ingestion_timestamp/source_system/batch_id"
            if not any(null_counts.values()) else f"null counts: {null_counts}",
        )  # fmt: skip
    )

    wrong_source = df.filter(F.col("source_system") != spec.source_system).count()
    detail = f"{wrong_source} rows with source_system != '{spec.source_system}'"
    results.append(CheckResult(name, "source_system_consistent", wrong_source == 0, detail))

    if spec.has_file_name:
        no_file = df.filter(F.col("file_name").isNull()).count()
        results.append(
            CheckResult(
                name, "file_name_populated", no_file == 0, f"{no_file} rows without file_name"
            )
        )
    return results
