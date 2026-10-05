"""Resolve where each lake table lives, for local disk or S3.

Pipeline code asks ``LakeLayout`` for a path and never builds paths by hand, so moving
from laptop to S3 is a configuration change, not a code change.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from edp.common.config import PROJECT_ROOT, Settings


class Layer(StrEnum):
    """Medallion layers plus supporting areas."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    QUARANTINE = "quarantine"  # rejected records, kept so nothing silently disappears
    AUDIT = "audit"  # pipeline run history and data-quality results


class LakeLayout:
    """Builds table locations such as ``<root>/silver/orders``."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @property
    def root(self) -> str:
        if self._settings.storage_backend == "s3":
            prefix = self._settings.s3_prefix.strip("/")
            base = f"s3a://{self._settings.s3_bucket}"
            return f"{base}/{prefix}" if prefix else base
        root = Path(self._settings.local_lake_root)
        if not root.is_absolute():
            root = PROJECT_ROOT / root
        return root.as_posix()

    def table_path(self, layer: Layer, table: str) -> str:
        """Location of a Delta table, e.g. ``table_path(Layer.SILVER, "orders")``."""
        if not table or "/" in table:
            raise ValueError(f"Invalid table name: {table!r}")
        return f"{self.root}/{layer.value}/{table}"
