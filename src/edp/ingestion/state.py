"""Pipeline state and audit store.

Holds the three things a restartable pipeline must remember:

* ``watermarks``       - how far each incremental dataset has been read
* ``processed_files``  - which source files were already landed (name + content hash)
* ``pipeline_runs``    - the audit trail: one row per run, success or failure

``StateStore`` is a Protocol so the backing technology can change (SQLite here for zero
infrastructure; PostgreSQL or DynamoDB in a shared production deployment) without touching
the runner. SQLite note: it is safe for the sequential/low-parallelism local setup used here,
including concurrent processes on one machine (WAL mode, busy timeout), but is not a good fit
for many writers across hosts.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from edp.ingestion.models import RunStatus, SourceFileRecord

SCHEMA = """
CREATE TABLE IF NOT EXISTS watermarks (
    source_system TEXT NOT NULL,
    dataset       TEXT NOT NULL,
    watermark     TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (source_system, dataset)
);
CREATE TABLE IF NOT EXISTS processed_files (
    source_system TEXT NOT NULL,
    dataset       TEXT NOT NULL,
    file_name     TEXT NOT NULL,
    sha256        TEXT NOT NULL,
    record_count  INTEGER NOT NULL,
    batch_id      TEXT NOT NULL,
    processed_at  TEXT NOT NULL,
    PRIMARY KEY (source_system, dataset, file_name, sha256)
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    pipeline_name     TEXT NOT NULL,
    source_system     TEXT NOT NULL,
    dataset           TEXT NOT NULL,
    batch_id          TEXT NOT NULL,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    status            TEXT NOT NULL,
    records_extracted INTEGER NOT NULL DEFAULT 0,
    records_processed INTEGER NOT NULL DEFAULT 0,
    records_rejected  INTEGER NOT NULL DEFAULT 0,
    watermark_from    TEXT,
    watermark_to      TEXT,
    error_type        TEXT,
    error_message     TEXT,
    details           TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_dataset ON pipeline_runs (source_system, dataset, started_at);
"""

MAX_ERROR_LENGTH = 2000


class StateStore(Protocol):
    def get_watermark(self, source_system: str, dataset: str) -> str | None: ...

    def is_file_processed(
        self, source_system: str, dataset: str, file_name: str, sha256: str
    ) -> bool: ...

    def start_run(
        self,
        *,
        run_id: str,
        pipeline_name: str,
        source_system: str,
        dataset: str,
        batch_id: str,
        started_at: datetime,
        watermark_from: str | None,
    ) -> None: ...

    def commit_extraction(
        self,
        *,
        source_system: str,
        dataset: str,
        run_id: str,
        batch_id: str,
        watermark: str | None,
        files: list[SourceFileRecord],
        now: datetime,
    ) -> None: ...

    def finish_run(
        self,
        *,
        run_id: str,
        status: RunStatus,
        finished_at: datetime,
        records_extracted: int,
        records_processed: int,
        records_rejected: int,
        watermark_to: str | None,
        error: BaseException | None = None,
        details: dict[str, Any] | None = None,
    ) -> None: ...


class SqliteStateStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._tx() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """One short-lived connection per operation; commits on success, rolls back on error."""
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------ incremental state

    def get_watermark(self, source_system: str, dataset: str) -> str | None:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT watermark FROM watermarks WHERE source_system = ? AND dataset = ?",
                (source_system, dataset),
            ).fetchone()
        return row["watermark"] if row else None

    def reset_watermark(self, source_system: str, dataset: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "DELETE FROM watermarks WHERE source_system = ? AND dataset = ?",
                (source_system, dataset),
            )

    def is_file_processed(
        self, source_system: str, dataset: str, file_name: str, sha256: str
    ) -> bool:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_files WHERE source_system = ? AND dataset = ? "
                "AND file_name = ? AND sha256 = ?",
                (source_system, dataset, file_name, sha256),
            ).fetchone()
        return row is not None

    def commit_extraction(
        self,
        *,
        source_system: str,
        dataset: str,
        run_id: str,
        batch_id: str,
        watermark: str | None,
        files: list[SourceFileRecord],
        now: datetime,
    ) -> None:
        """Advance the watermark and record landed files in ONE transaction, so the two can
        never disagree about what has been ingested."""
        with self._tx() as conn:
            if watermark is not None:
                conn.execute(
                    "INSERT INTO watermarks "
                    "(source_system, dataset, watermark, run_id, updated_at) "
                    "VALUES (?, ?, ?, ?, ?) ON CONFLICT (source_system, dataset) DO UPDATE SET "
                    "watermark = excluded.watermark, run_id = excluded.run_id, "
                    "updated_at = excluded.updated_at",
                    (source_system, dataset, watermark, run_id, now.isoformat()),
                )
            conn.executemany(
                "INSERT OR REPLACE INTO processed_files (source_system, dataset, file_name, "
                "sha256, record_count, batch_id, processed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (source_system, dataset, f.file_name, f.sha256, f.record_count, batch_id,
                     now.isoformat())
                    for f in files
                ],
            )  # fmt: skip

    # ------------------------------------------------------------------------------ audit

    def start_run(
        self,
        *,
        run_id: str,
        pipeline_name: str,
        source_system: str,
        dataset: str,
        batch_id: str,
        started_at: datetime,
        watermark_from: str | None,
    ) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (run_id, pipeline_name, source_system, dataset, "
                "batch_id, started_at, status, watermark_from) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, pipeline_name, source_system, dataset, batch_id,
                 started_at.isoformat(), RunStatus.RUNNING.value, watermark_from),
            )  # fmt: skip

    def finish_run(
        self,
        *,
        run_id: str,
        status: RunStatus,
        finished_at: datetime,
        records_extracted: int,
        records_processed: int,
        records_rejected: int,
        watermark_to: str | None,
        error: BaseException | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE pipeline_runs SET status = ?, finished_at = ?, records_extracted = ?, "
                "records_processed = ?, records_rejected = ?, watermark_to = ?, error_type = ?, "
                "error_message = ?, details = ? WHERE run_id = ?",
                (
                    status.value,
                    finished_at.isoformat(),
                    records_extracted,
                    records_processed,
                    records_rejected,
                    watermark_to,
                    type(error).__name__ if error else None,
                    str(error)[:MAX_ERROR_LENGTH] if error else None,
                    json.dumps(details) if details else None,
                    run_id,
                ),
            )

    def list_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        """Most recent runs first (used by tests now, by monitoring views later)."""
        with self._tx() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC, rowid DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
