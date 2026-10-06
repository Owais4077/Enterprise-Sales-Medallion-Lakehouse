"""Bronze loader: landing-zone batches -> Delta tables.

Rules that define "Bronze" in this project:

* **Source values are not changed.** Every source column is stored as the exact text that was
  landed (decimals as ``"19.90"``, timestamps as ISO strings, a bad rating as ``"abc"``).
  A type error can therefore never reject or lose a record at this layer; typing, cleaning and
  validation belong to Silver, and Bronze lets Silver reprocess history at any time.
* **Lineage is added.** ``ingestion_timestamp``, ``source_system``, ``batch_id``, ``file_name``
  (file sources only) and ``run_id`` are appended to every row.
* **Loads are idempotent.** Each batch becomes one ``batch_id`` partition, written by a single
  atomic Delta commit. A batch already present is skipped, so re-running loads nothing twice.
* **Landing data is verified first**: SHA-256 checksums must match the manifest and the row
  count read by Spark must equal the manifest's ``record_count`` before anything is written.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from edp.common.paths import LakeLayout, Layer
from edp.ingestion.config import IngestionConfig
from edp.ingestion.landing import MANIFEST_NAME, sha256_file
from edp.ingestion.models import RunStatus
from edp.ingestion.state import StateStore

logger = logging.getLogger(__name__)

Kind = Literal["jsonl", "json", "csv"]
METADATA_COLUMNS = ("ingestion_timestamp", "source_system", "batch_id", "file_name", "run_id")
PIPELINE_NAME = "bronze_load"


class BronzeLoadError(Exception):
    """A batch could not be loaded safely. Nothing was written for it."""


@dataclass(frozen=True)
class DatasetSpec:
    """Where a dataset comes from and how its landed files are formatted."""

    source_system: str
    dataset: str
    kind: Kind  # jsonl = our own row batches; json/csv = raw source files

    @property
    def has_file_name(self) -> bool:
        return self.kind != "jsonl"


@dataclass(frozen=True)
class LandedBatch:
    spec: DatasetSpec
    batch_id: str
    path: Path
    manifest: dict[str, Any]

    @property
    def record_count(self) -> int:
        return int(self.manifest["record_count"])

    @property
    def extracted_at(self) -> datetime:
        return datetime.fromisoformat(self.manifest["extracted_at"])


@dataclass(frozen=True)
class BatchLoadResult:
    dataset: str
    batch_id: str
    rows_loaded: int


def dataset_specs(config: IngestionConfig) -> dict[str, DatasetSpec]:
    """One spec per configured dataset, derived from the same config the ingestion uses."""
    specs: dict[str, DatasetSpec] = {}
    for name in config.tables:
        specs[name] = DatasetSpec("postgres", name, "jsonl")
    for name in config.api_datasets:
        specs[name] = DatasetSpec("rest_api", name, "jsonl")
    for name, files in config.file_datasets.items():
        specs[name] = DatasetSpec("files", name, files.file_format)  # type: ignore[arg-type]
    return specs


def discover_batches(landing_root: Path, spec: DatasetSpec) -> list[LandedBatch]:
    """Committed batches (those with a manifest), oldest first. Hidden ``.tmp-`` folders and
    folders without a manifest are in-progress or failed writes and are ignored."""
    folder = landing_root / spec.source_system / spec.dataset
    batches = []
    for path in sorted(folder.glob("batch_id=*")):
        manifest_path = path / MANIFEST_NAME
        if not manifest_path.is_file():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        batch_id = path.name.removeprefix("batch_id=")
        if manifest.get("batch_id") != batch_id:
            raise BronzeLoadError(f"Manifest/folder batch_id mismatch in {path}")
        batches.append(LandedBatch(spec, batch_id, path, manifest))
    return batches


def verify_integrity(batch: LandedBatch) -> None:
    """Re-hash every landed file and compare with the manifest."""
    expected = [(f["name"], f["sha256"]) for f in batch.manifest.get("data_files", [])]
    expected += [(f["file_name"], f["sha256"]) for f in batch.manifest.get("source_files", [])]
    for name, digest in expected:
        file = batch.path / name
        if not file.is_file():
            raise BronzeLoadError(f"[{batch.batch_id}] landed file missing: {name}")
        if sha256_file(file) != digest:
            raise BronzeLoadError(f"[{batch.batch_id}] checksum mismatch for {name}")


class BronzeLoader:
    def __init__(
        self,
        spark: SparkSession,
        layout: LakeLayout,
        landing_root: Path,
        state: StateStore | None = None,
    ) -> None:
        self._spark = spark
        self._layout = layout
        self._landing_root = landing_root
        self._state = state

    def table_path(self, dataset: str) -> str:
        return self._layout.table_path(Layer.BRONZE, dataset)

    def table_exists(self, dataset: str) -> bool:
        return DeltaTable.isDeltaTable(self._spark, self.table_path(dataset))

    def loaded_batch_ids(self, dataset: str) -> set[str]:
        """Batches already in Bronze. The table itself is the source of truth, so the answer
        can never disagree with the data (no separate bookkeeping to get out of sync)."""
        if not self.table_exists(dataset):
            return set()
        rows = self._spark.read.format("delta").load(self.table_path(dataset))
        return {r["batch_id"] for r in rows.select("batch_id").distinct().collect()}

    # ------------------------------------------------------------------------------ read

    def _read(self, batch: LandedBatch) -> DataFrame:
        reader = self._spark.read
        spec = batch.spec
        if spec.kind == "jsonl":
            paths = [str(batch.path / f["name"]) for f in batch.manifest["data_files"]]
            df = reader.option("primitivesAsString", "true").json(paths)
        else:
            paths = [str(batch.path / f["file_name"]) for f in batch.manifest["source_files"]]
            if spec.kind == "json":
                df = reader.option("multiLine", "true").option("primitivesAsString", "true")
                df = df.json(paths)
            else:
                df = (
                    reader.option("header", "true")
                    .option("inferSchema", "false")  # keep everything as text
                    .option("multiLine", "true")
                    .option("escape", '"')  # standard CSV quoting ("" inside quotes)
                    .csv(paths)
                )
        if "_corrupt_record" in df.columns:
            raise BronzeLoadError(f"[{batch.batch_id}] unparseable records in landed data")
        clash = set(METADATA_COLUMNS) & set(df.columns)
        if clash:
            raise BronzeLoadError(
                f"[{batch.batch_id}] source columns collide with Bronze metadata: {sorted(clash)}"
            )
        return df

    def _with_metadata(self, df: DataFrame, batch: LandedBatch) -> DataFrame:
        file_name = (
            F.regexp_extract(F.input_file_name(), r"[^/]+$", 0)
            if batch.spec.has_file_name
            else F.lit(None).cast("string")
        )
        return (
            df.withColumn("ingestion_timestamp", F.lit(batch.extracted_at).cast("timestamp"))
            .withColumn("source_system", F.lit(batch.spec.source_system))
            .withColumn("batch_id", F.lit(batch.batch_id))
            .withColumn("file_name", file_name)
            .withColumn("run_id", F.lit(batch.manifest.get("run_id")))
        )

    # ----------------------------------------------------------------------------- write

    def load_batch(self, batch: LandedBatch) -> BatchLoadResult | None:
        """Verify, count-check, then append one batch in a single Delta commit."""
        if batch.record_count == 0:
            logger.info("[%s] batch %s has 0 records; nothing to load", batch.spec.dataset,
                        batch.batch_id)  # fmt: skip
            return None
        verify_integrity(batch)
        df = self._with_metadata(self._read(batch), batch)
        rows = df.count()
        if rows != batch.record_count:
            raise BronzeLoadError(
                f"[{batch.batch_id}] Spark read {rows} rows but manifest says {batch.record_count}"
            )
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")  # additive schema evolution: new source columns
            .partitionBy("batch_id")
            .save(self.table_path(batch.spec.dataset))
        )
        logger.info("[%s] loaded batch %s (%d rows)", batch.spec.dataset, batch.batch_id, rows)
        return BatchLoadResult(batch.spec.dataset, batch.batch_id, rows)

    def load_dataset(self, spec: DatasetSpec) -> list[BatchLoadResult]:
        """Load every not-yet-loaded batch of a dataset, oldest first.

        Stops at the first failing batch so ordering is preserved: a later batch must never
        land before an earlier one that is still broken.
        """
        done = self.loaded_batch_ids(spec.dataset)
        pending = [b for b in discover_batches(self._landing_root, spec) if b.batch_id not in done]
        if not pending:
            logger.info("[%s] up to date (%d batches already loaded)", spec.dataset, len(done))
            return []
        results: list[BatchLoadResult] = []
        for batch in pending:
            run_id = self._audit_start(batch)
            try:
                result = self.load_batch(batch)
            except Exception as exc:
                self._audit_finish(run_id, RunStatus.FAILED, batch, 0, exc)
                logger.exception("[%s] batch %s FAILED", spec.dataset, batch.batch_id)
                raise
            self._audit_finish(
                run_id, RunStatus.SUCCESS, batch, result.rows_loaded if result else 0
            )
            if result:
                results.append(result)
        return results

    # ----------------------------------------------------------------------------- audit

    def _audit_start(self, batch: LandedBatch) -> str | None:
        if self._state is None:
            return None
        run_id = str(uuid.uuid4())
        self._state.start_run(
            run_id=run_id, pipeline_name=PIPELINE_NAME, source_system=batch.spec.source_system,
            dataset=batch.spec.dataset, batch_id=batch.batch_id,
            started_at=datetime.now(UTC), watermark_from=None,
        )  # fmt: skip
        return run_id

    def _audit_finish(
        self,
        run_id: str | None,
        status: RunStatus,
        batch: LandedBatch,
        rows: int,
        error: BaseException | None = None,
    ) -> None:
        if self._state is None or run_id is None:
            return
        self._state.finish_run(
            run_id=run_id, status=status, finished_at=datetime.now(UTC),
            records_extracted=batch.record_count, records_processed=rows,
            records_rejected=0, watermark_to=None, error=error,
        )  # fmt: skip
