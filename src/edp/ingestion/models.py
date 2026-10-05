"""Plain data containers passed between ingestion components."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"  # finished, but some inputs (files) were rejected
    FAILED = "failed"


@dataclass(frozen=True)
class SourceFileRecord:
    """A source file that was landed unchanged."""

    file_name: str
    sha256: str
    record_count: int
    size_bytes: int


@dataclass(frozen=True)
class RejectedFile:
    file_name: str
    reason: str


@dataclass
class ExtractionStats:
    """What an extractor reports back to the runner."""

    records_extracted: int = 0
    # New high-water mark to store after a successful commit. None = leave unchanged.
    next_watermark: str | None = None
    source_files: list[SourceFileRecord] = field(default_factory=list)
    rejected_files: list[RejectedFile] = field(default_factory=list)


@dataclass(frozen=True)
class RunResult:
    """Outcome of one extractor run (also what is written to the audit table)."""

    run_id: str
    pipeline_name: str
    source_system: str
    dataset: str
    batch_id: str
    status: RunStatus
    records_extracted: int
    records_processed: int
    records_rejected: int
    watermark_from: str | None
    watermark_to: str | None
    landing_path: Path | None
    duration_seconds: float
    rejected_files: tuple[RejectedFile, ...] = ()
