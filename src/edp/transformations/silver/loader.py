"""Silver loader: reads Bronze Delta tables, cleans data, writes quarantine,
and MERGEs into Silver Delta tables.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from edp.common.paths import LakeLayout, Layer
from edp.ingestion.models import RunStatus
from edp.ingestion.state import StateStore
from edp.transformations.silver.cleaner import clean_and_split
from edp.transformations.silver.schemas import SILVER_DATASET_SPECS, DatasetSilverSpec

logger = logging.getLogger(__name__)

PIPELINE_NAME = "silver_load"


class SilverLoadError(Exception):
    """Raised when a dataset cannot be transformed or merged into Silver safely."""


@dataclass(frozen=True)
class SilverLoadResult:
    dataset: str
    rows_processed: int
    rows_valid: int
    rows_quarantined: int


def deduplicate_incremental(df: DataFrame, spec: DatasetSilverSpec) -> DataFrame:
    """Deduplicates input DataFrame by primary key, keeping the newest record
    based on watermark_col or ingestion_timestamp.
    """
    if not spec.primary_key:
        return df

    order_col = (
        spec.watermark_col
        if (spec.watermark_col and spec.watermark_col in df.columns)
        else "ingestion_timestamp"
    )

    window_spec = Window.partitionBy(*spec.primary_key).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


class SilverLoader:
    def __init__(
        self,
        spark: SparkSession,
        layout: LakeLayout,
        state: StateStore | None = None,
    ) -> None:
        self._spark = spark
        self._layout = layout
        self._state = state

    def bronze_table_path(self, dataset: str) -> str:
        return self._layout.table_path(Layer.BRONZE, dataset)

    def silver_table_path(self, dataset: str) -> str:
        return self._layout.table_path(Layer.SILVER, dataset)

    def quarantine_table_path(self, dataset: str) -> str:
        return f"{self._layout.table_path(Layer.SILVER, '_quarantine')}/{dataset}"

    def silver_table_exists(self, dataset: str) -> bool:
        return DeltaTable.isDeltaTable(self._spark, self.silver_table_path(dataset))

    def bronze_table_exists(self, dataset: str) -> bool:
        return DeltaTable.isDeltaTable(self._spark, self.bronze_table_path(dataset))

    def loaded_batch_ids(self, dataset: str) -> set[str]:
        """Reads batch_ids already present in Silver table and Quarantine table."""
        loaded = set()
        if self.silver_table_exists(dataset):
            rows = self._spark.read.format("delta").load(self.silver_table_path(dataset))
            if "batch_id" in rows.columns:
                loaded.update({r["batch_id"] for r in rows.select("batch_id").distinct().collect()})

        quarantine_path = self.quarantine_table_path(dataset)
        if DeltaTable.isDeltaTable(self._spark, quarantine_path):
            q_rows = self._spark.read.format("delta").load(quarantine_path)
            if "batch_id" in q_rows.columns:
                loaded.update(
                    {r["batch_id"] for r in q_rows.select("batch_id").distinct().collect()}
                )

        return loaded

    def merge_to_silver(self, valid_df: DataFrame, spec: DatasetSilverSpec) -> None:
        """Executes Delta MERGE INTO target Silver table or creates table if missing."""
        deduped_df = deduplicate_incremental(valid_df, spec)
        target_path = self.silver_table_path(spec.dataset)

        if not self.silver_table_exists(spec.dataset):
            logger.info(
                "[%s] Silver table does not exist. Creating at %s", spec.dataset, target_path
            )
            (
                deduped_df.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(target_path)
            )
            return

        delta_table = DeltaTable.forPath(self._spark, target_path)
        pk_match_cond = " AND ".join([f"target.{col} = source.{col}" for col in spec.primary_key])

        order_col = (
            spec.watermark_col
            if (spec.watermark_col and spec.watermark_col in deduped_df.columns)
            else "ingestion_timestamp"
        )
        update_cond = f"source.{order_col} >= target.{order_col}"

        (
            delta_table.alias("target")
            .merge(deduped_df.alias("source"), pk_match_cond)
            .whenMatchedUpdateAll(condition=update_cond)
            .whenNotMatchedInsertAll()
            .execute()
        )

    def write_quarantine(self, quarantine_df: DataFrame, spec: DatasetSilverSpec) -> None:
        """Appends invalid records to quarantine Delta table."""
        if quarantine_df.count() == 0:
            return
        target_path = self.quarantine_table_path(spec.dataset)
        logger.warning(
            "[%s] Writing %d records to quarantine at %s",
            spec.dataset,
            quarantine_df.count(),
            target_path,
        )
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_path)
        )

    def load_dataset(self, dataset: str) -> SilverLoadResult | None:
        """Processes pending Bronze batches into Silver Delta table for a dataset."""
        spec = SILVER_DATASET_SPECS.get(dataset)
        if spec is None:
            raise SilverLoadError(f"Unknown dataset spec: {dataset}")

        if not self.bronze_table_exists(dataset):
            logger.info("[%s] No Bronze Delta table found; skipping Silver load.", dataset)
            return None

        already_loaded = self.loaded_batch_ids(dataset)
        bronze_df = self._spark.read.format("delta").load(self.bronze_table_path(dataset))

        if "batch_id" in bronze_df.columns and already_loaded:
            pending_df = bronze_df.filter(~F.col("batch_id").isin(list(already_loaded)))
        else:
            pending_df = bronze_df

        total_rows = pending_df.count()
        if total_rows == 0:
            logger.info("[%s] Silver layer up to date.", dataset)
            return SilverLoadResult(dataset, 0, 0, 0)

        run_id = self._audit_start(spec, total_rows)
        try:
            valid_df, quarantine_df = clean_and_split(pending_df, spec)
            valid_cnt = valid_df.count()
            quarantine_cnt = quarantine_df.count()

            if quarantine_cnt > 0:
                self.write_quarantine(quarantine_df, spec)

            if valid_cnt > 0:
                self.merge_to_silver(valid_df, spec)

            self._audit_finish(
                run_id, RunStatus.SUCCESS, spec, total_rows, valid_cnt, quarantine_cnt
            )
            logger.info(
                "[%s] Silver load finished. Processed: %d, Valid: %d, Quarantined: %d",
                dataset,
                total_rows,
                valid_cnt,
                quarantine_cnt,
            )
            return SilverLoadResult(dataset, total_rows, valid_cnt, quarantine_cnt)

        except Exception as exc:
            self._audit_finish(run_id, RunStatus.FAILED, spec, total_rows, 0, 0, exc)
            logger.exception("[%s] Silver load FAILED", dataset)
            raise

    # ----------------------------------------------------------------------------- audit

    def _audit_start(self, spec: DatasetSilverSpec, rows: int) -> str | None:
        if self._state is None:
            return None
        run_id = str(uuid.uuid4())
        self._state.start_run(
            run_id=run_id,
            pipeline_name=PIPELINE_NAME,
            source_system="bronze",
            dataset=spec.dataset,
            batch_id="incremental",
            started_at=datetime.now(UTC),
            watermark_from=None,
        )
        return run_id

    def _audit_finish(
        self,
        run_id: str | None,
        status: RunStatus,
        spec: DatasetSilverSpec,
        extracted: int,
        valid: int,
        quarantined: int,
        error: BaseException | None = None,
    ) -> None:
        if self._state is None or run_id is None:
            return
        self._state.finish_run(
            run_id=run_id,
            status=status,
            finished_at=datetime.now(UTC),
            records_extracted=extracted,
            records_processed=valid,
            records_rejected=quarantined,
            watermark_to=None,
            error=error,
        )
