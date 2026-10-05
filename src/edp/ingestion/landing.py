"""Landing zone: where extracted data waits for the Bronze load.

Layout::

    <landing>/<source_system>/<dataset>/batch_id=<id>/
        part-00000.jsonl.gz ...    row data (database / API sources)
        <original file name>       source files, byte-for-byte copies (file sources)
        _manifest.json             what is in the batch; written last

A batch is built in a hidden ``.tmp-`` directory and renamed into place only when complete.
A reader (Bronze) therefore sees either a whole batch or nothing - never a half-written one.
Source values are written losslessly (decimals and timestamps as text) and are not changed;
typing and cleaning belong to Silver.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import shutil
import uuid
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from edp.ingestion.models import SourceFileRecord

logger = logging.getLogger(__name__)

MANIFEST_NAME = "_manifest.json"
HASH_CHUNK = 1024 * 1024


def make_batch_id(now: datetime) -> str:
    """Sortable, unique id such as ``20260105T101500Z-3fa9c1d2``."""
    return f"{now.astimezone(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_default(value: Any) -> Any:
    """Lossless encoding for values JSON cannot represent. Unknown types fail loudly."""
    if isinstance(value, Decimal):
        return str(value)  # a float would silently round money
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"Cannot serialise {type(value).__name__}: {value!r}")


class LandingBatch:
    """Builder for one batch. Use ``commit()`` or ``abort()``; never leaves partial output."""

    def __init__(
        self,
        root: Path,
        source_system: str,
        dataset: str,
        batch_id: str,
        max_rows_per_file: int = 100_000,
    ) -> None:
        self.source_system, self.dataset, self.batch_id = source_system, dataset, batch_id
        self._max_rows = max_rows_per_file
        parent = root / source_system / dataset
        self.final_dir = parent / f"batch_id={batch_id}"
        self._tmp_dir = parent / f".tmp-batch_id={batch_id}"
        if self.final_dir.exists() or self._tmp_dir.exists():
            raise FileExistsError(f"Batch already exists: {batch_id}")
        self._tmp_dir.mkdir(parents=True)
        self._part_files: list[dict[str, Any]] = []
        self._source_files: list[SourceFileRecord] = []
        self._handle: gzip.GzipFile | Any = None
        self._current_name = ""
        self._current_rows = 0
        self.rows_written = 0
        self._committed = False

    # -------------------------------------------------------------------------- writing

    def write_rows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        for row in rows:
            if self._handle is None or self._current_rows >= self._max_rows:
                self._open_next_part()
            self._handle.write(
                json.dumps(row, default=_json_default, ensure_ascii=False, allow_nan=False,
                           separators=(",", ":"))
                + "\n"
            )  # fmt: skip
            self._current_rows += 1
            self.rows_written += 1

    def add_file(self, source: Path, *, record_count: int, sha256: str) -> None:
        """Copy a source file unchanged into the batch."""
        target = self._tmp_dir / source.name
        shutil.copyfile(source, target)
        self._source_files.append(
            SourceFileRecord(source.name, sha256, record_count, target.stat().st_size)
        )

    def _open_next_part(self) -> None:
        self._close_part()
        self._current_name = f"part-{len(self._part_files):05d}.jsonl.gz"
        # Handle intentionally spans many write_rows() calls; closed by _close_part()/abort().
        self._handle = gzip.open(  # noqa: SIM115
            self._tmp_dir / self._current_name, "wt", encoding="utf-8", compresslevel=3,
            newline="\n",
        )  # fmt: skip
        self._current_rows = 0

    def _close_part(self) -> None:
        if self._handle is None:
            return
        self._handle.close()
        self._handle = None
        path = self._tmp_dir / self._current_name
        self._part_files.append(
            {
                "name": self._current_name,
                "rows": self._current_rows,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    @property
    def is_empty(self) -> bool:
        return self.rows_written == 0 and not self._source_files

    # ------------------------------------------------------------------------ finishing

    def commit(self, manifest_extra: Mapping[str, Any]) -> Path:
        """Write the manifest last, then atomically publish the batch directory."""
        self._close_part()
        manifest = {
            "batch_id": self.batch_id,
            "source_system": self.source_system,
            "dataset": self.dataset,
            "record_count": self.rows_written + sum(f.record_count for f in self._source_files),
            "data_files": self._part_files,
            "source_files": [vars(f) for f in self._source_files],
            **manifest_extra,
        }
        (self._tmp_dir / MANIFEST_NAME).write_text(
            json.dumps(manifest, indent=2, default=_json_default), encoding="utf-8"
        )
        self._tmp_dir.rename(self.final_dir)
        self._committed = True
        logger.info("Committed batch %s (%d records)", self.final_dir, manifest["record_count"])
        return self.final_dir

    def abort(self) -> None:
        """Discard everything written so far. Safe to call repeatedly and after commit."""
        if self._committed:
            return
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def remove_committed(self) -> None:
        """Undo a commit (used if bookkeeping fails right after publishing)."""
        shutil.rmtree(self.final_dir, ignore_errors=True)
        self._committed = False
