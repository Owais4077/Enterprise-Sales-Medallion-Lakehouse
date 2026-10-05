"""CSV / JSON file extractor.

Files are landed **byte-for-byte unchanged** (raw means raw). The extractor only

* skips files already landed, identified by ``(file name, sha256)``: a re-delivered identical
  file is ignored, a file whose content changed under the same name is landed again;
* checks each file is structurally sound before landing it and counts its records;
* reports structurally broken files as rejected instead of failing the whole run, so one bad
  monthly file cannot block the other eleven. Rejected files are not marked processed, so they
  are reported again on every run until fixed - they never disappear silently.

Row-level problems *inside* a valid file (bad ratings, orphan keys) are Silver's responsibility.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

from edp.ingestion.base import ExtractionContext, Extractor
from edp.ingestion.config import FileDatasetConfig
from edp.ingestion.errors import ConfigurationError, InvalidSourceFile
from edp.ingestion.landing import sha256_file
from edp.ingestion.models import ExtractionStats, RejectedFile, SourceFileRecord

logger = logging.getLogger(__name__)


def validate_csv(path: Path, required_columns: tuple[str, ...]) -> int:
    """Return the data-row count, or raise ``InvalidSourceFile``."""
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                raise InvalidSourceFile("file is empty or has no header")
            missing = [c for c in required_columns if c not in header]
            if missing:
                raise InvalidSourceFile(f"missing required columns: {missing}")
            count = 0
            for line_number, row in enumerate(reader, start=2):
                if not row:
                    continue  # tolerate blank lines
                if len(row) != len(header):
                    raise InvalidSourceFile(
                        f"line {line_number} has {len(row)} fields, expected {len(header)}"
                    )
                count += 1
            return count
    except UnicodeDecodeError as exc:
        raise InvalidSourceFile(f"not valid UTF-8: {exc}") from exc
    except csv.Error as exc:
        raise InvalidSourceFile(f"malformed CSV: {exc}") from exc


def validate_json(path: Path) -> int:
    """Expect a JSON array of objects. Return its length, or raise ``InvalidSourceFile``."""
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except UnicodeDecodeError as exc:
        raise InvalidSourceFile(f"not valid UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise InvalidSourceFile(f"malformed JSON: {exc}") from exc
    if not isinstance(data, list):
        raise InvalidSourceFile("top level must be a JSON array of records")
    if not all(isinstance(item, dict) for item in data):
        raise InvalidSourceFile("every array element must be a JSON object")
    return len(data)


class FileExtractor(Extractor):
    source_system = "files"

    def __init__(self, *, dataset: FileDatasetConfig, root: Path) -> None:
        self.dataset = dataset.name
        self._config = dataset
        self._directory = root / dataset.directory

    def _validate(self, path: Path) -> int:
        if self._config.file_format == "csv":
            return validate_csv(path, self._config.required_columns)
        return validate_json(path)

    def extract(self, ctx: ExtractionContext) -> ExtractionStats:
        if not self._directory.is_dir():
            raise ConfigurationError(
                f"Source directory not found: {self._directory} "
                "(generate it with: python -m data_generation.generate files)"
            )
        stats = ExtractionStats()
        skipped = 0
        for path in sorted(self._directory.glob(self._config.pattern)):
            if not path.is_file():
                continue
            digest = sha256_file(path)
            if not ctx.full_refresh and ctx.state.is_file_processed(
                self.source_system, self.dataset, path.name, digest
            ):
                skipped += 1
                continue
            try:
                count = self._validate(path)
            except InvalidSourceFile as exc:
                logger.warning("[files.%s] REJECTED %s: %s", self.dataset, path.name, exc)
                stats.rejected_files.append(RejectedFile(path.name, str(exc)))
                continue
            ctx.sink.add_file(path, record_count=count, sha256=digest)
            stats.source_files.append(
                SourceFileRecord(path.name, digest, count, path.stat().st_size)
            )
            stats.records_extracted += count
        logger.info(
            "[files.%s] %d new file(s), %d already processed, %d rejected, %d records",
            self.dataset, len(stats.source_files), skipped, len(stats.rejected_files),
            stats.records_extracted,
        )  # fmt: skip
        return stats
