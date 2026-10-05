"""Runs an extractor with uniform guarantees.

Order of operations (the order is the point):

1. read the stored watermark
2. record the run as ``running`` in the audit table
3. extractor writes into a hidden temporary batch
4. publish the batch atomically (rename)
5. advance the watermark + file bookkeeping in ONE transaction
6. mark the run ``success`` / ``partial`` in the audit table

If anything fails before step 5 completes, the watermark has not moved and the temp batch is
deleted, so the next run simply retries the same window. If the process is killed between
steps 4 and 5, the batch exists but the watermark did not advance: the next run lands the same
rows again (**at-least-once**). Downstream layers deduplicate on key + ``updated_at``.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from edp.ingestion.base import ExtractionContext, Extractor
from edp.ingestion.landing import LandingBatch, make_batch_id
from edp.ingestion.models import RunResult, RunStatus
from edp.ingestion.state import StateStore

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class IngestionRunner:
    def __init__(
        self,
        *,
        state: StateStore,
        landing_root: Path,
        pipeline_name: str = "ingestion",
        max_rows_per_file: int = 100_000,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._state = state
        self._landing_root = landing_root
        self._pipeline_name = pipeline_name
        self._max_rows = max_rows_per_file
        self._clock = clock

    def run(
        self,
        extractor: Extractor,
        *,
        full_refresh: bool = False,
        batch_id: str | None = None,
        run_id: str | None = None,
    ) -> RunResult:
        """Run one extractor. Raises on failure (after recording it in the audit table)."""
        source, dataset = extractor.source_system, extractor.dataset
        run_id = run_id or str(uuid.uuid4())
        started = self._clock()
        started_clock = time.monotonic()
        batch_id = batch_id or make_batch_id(started)
        since = None if full_refresh else self._state.get_watermark(source, dataset)
        label = f"{source}.{dataset}"

        self._state.start_run(
            run_id=run_id, pipeline_name=self._pipeline_name, source_system=source,
            dataset=dataset, batch_id=batch_id, started_at=started, watermark_from=since,
        )  # fmt: skip
        logger.info(
            "[%s] run %s started (%s, since=%s)",
            label, run_id, "full refresh" if full_refresh else "incremental", since,
        )  # fmt: skip

        batch = LandingBatch(self._landing_root, source, dataset, batch_id, self._max_rows)
        extracted = processed = 0
        state_committed = False
        try:
            stats = extractor.extract(
                ExtractionContext(
                    since=since, full_refresh=full_refresh, sink=batch, state=self._state
                )
            )
            extracted = stats.records_extracted
            processed = batch.rows_written + sum(f.record_count for f in stats.source_files)
            watermark_to = stats.next_watermark or since

            landing_path: Path | None = None
            if batch.is_empty:
                batch.abort()  # never publish empty batches
            else:
                landing_path = batch.commit(
                    {
                        "run_id": run_id,
                        "pipeline_name": self._pipeline_name,
                        "mode": "full_refresh" if full_refresh else "incremental",
                        "extracted_at": self._clock().isoformat(),
                        "watermark_from": since,
                        "watermark_to": watermark_to,
                        "rejected_files": [vars(r) for r in stats.rejected_files],
                    }
                )

            self._state.commit_extraction(
                source_system=source, dataset=dataset, run_id=run_id, batch_id=batch_id,
                watermark=stats.next_watermark, files=stats.source_files, now=self._clock(),
            )  # fmt: skip
            state_committed = True

            status = RunStatus.PARTIAL if stats.rejected_files else RunStatus.SUCCESS
            self._state.finish_run(
                run_id=run_id, status=status, finished_at=self._clock(),
                records_extracted=extracted, records_processed=processed, records_rejected=0,
                watermark_to=watermark_to,
                details={"rejected_files": [vars(r) for r in stats.rejected_files]}
                if stats.rejected_files else None,
            )  # fmt: skip
        except BaseException as exc:
            batch.abort()
            if landing_path_published(batch) and not state_committed:
                # Published but not recorded: remove it so state and landing stay consistent.
                batch.remove_committed()
            logger.exception("[%s] run %s FAILED", label, run_id)
            self._record_failure(run_id, exc, extracted, processed, since)
            raise
        finally:
            extractor.close()

        result = RunResult(
            run_id=run_id, pipeline_name=self._pipeline_name, source_system=source,
            dataset=dataset, batch_id=batch_id, status=status, records_extracted=extracted,
            records_processed=processed, records_rejected=0, watermark_from=since,
            watermark_to=watermark_to, landing_path=landing_path,
            duration_seconds=round(time.monotonic() - started_clock, 3),
            rejected_files=tuple(stats.rejected_files),
        )  # fmt: skip
        logger.info(
            "[%s] run %s %s: %d extracted, %d landed in %.1fs",
            label, run_id, status.value.upper(), extracted, processed, result.duration_seconds,
        )  # fmt: skip
        return result

    def _record_failure(
        self,
        run_id: str,
        error: BaseException,
        extracted: int,
        processed: int,
        since: str | None,
    ) -> None:
        """Audit the failure. A failure to audit must not hide the original error."""
        try:
            self._state.finish_run(
                run_id=run_id, status=RunStatus.FAILED, finished_at=self._clock(),
                records_extracted=extracted, records_processed=0, records_rejected=0,
                watermark_to=since, error=error,
            )  # fmt: skip
        except Exception:
            logger.exception("Could not write failure audit for run %s", run_id)


def landing_path_published(batch: LandingBatch) -> bool:
    return batch.final_dir.exists()
