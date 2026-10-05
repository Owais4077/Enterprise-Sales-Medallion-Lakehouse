"""Typed view of the ``ingestion`` and ``sources`` sections of ``config/pipeline.yaml``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from edp.ingestion.errors import ConfigurationError
from edp.ingestion.retry import RetryPolicy


@dataclass(frozen=True)
class TableConfig:
    name: str
    primary_key: tuple[str, ...]
    watermark_column: str


@dataclass(frozen=True)
class ApiDatasetConfig:
    name: str
    endpoint: str
    watermark_field: str
    primary_key: tuple[str, ...]


@dataclass(frozen=True)
class FileDatasetConfig:
    name: str
    directory: str
    pattern: str
    file_format: str  # "csv" | "json"
    required_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class IngestionConfig:
    batch_size: int
    max_rows_per_file: int
    watermark_lag_seconds: float
    retry: RetryPolicy
    api_timeout_seconds: float
    api_page_size: int
    pg_schema: str
    tables: dict[str, TableConfig]
    api_datasets: dict[str, ApiDatasetConfig]
    file_datasets: dict[str, FileDatasetConfig]

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> IngestionConfig:
        """Build from the parsed YAML. Raises ``ConfigurationError`` with a clear message."""
        try:
            ing, src = raw["ingestion"], raw["sources"]
            retry = ing["retry"]
            config = cls(
                batch_size=int(ing["batch_size"]),
                max_rows_per_file=int(ing["max_rows_per_file"]),
                watermark_lag_seconds=float(ing["watermark_lag_seconds"]),
                retry=RetryPolicy(
                    max_attempts=int(retry["max_attempts"]),
                    base_delay=float(retry["base_delay_seconds"]),
                    max_delay=float(retry["max_delay_seconds"]),
                    jitter=float(retry["jitter"]),
                ),
                api_timeout_seconds=float(ing["api"]["timeout_seconds"]),
                api_page_size=int(ing["api"]["page_size"]),
                pg_schema=str(src["postgres"]["schema"]),
                tables={
                    name: TableConfig(
                        name=name,
                        primary_key=_as_tuple(spec["primary_key"]),
                        watermark_column=str(spec["watermark_column"]),
                    )
                    for name, spec in src["postgres"]["tables"].items()
                },
                api_datasets={
                    name: ApiDatasetConfig(
                        name=name,
                        endpoint=str(spec["endpoint"]),
                        watermark_field=str(spec["watermark_field"]),
                        primary_key=_as_tuple(spec["primary_key"]),
                    )
                    for name, spec in src["api"].items()
                },
                file_datasets={
                    name: FileDatasetConfig(
                        name=name,
                        directory=str(spec["directory"]),
                        pattern=str(spec["pattern"]),
                        file_format=str(spec["format"]),
                        required_columns=_as_tuple(spec.get("required_columns", ())),
                    )
                    for name, spec in src["files"].items()
                },
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfigurationError(f"Invalid ingestion configuration: {exc!r}") from exc
        if config.batch_size < 1 or config.max_rows_per_file < 1 or config.api_page_size < 1:
            raise ConfigurationError("batch_size, max_rows_per_file and page_size must be >= 1")
        for dataset in config.file_datasets.values():
            if dataset.file_format not in ("csv", "json"):
                raise ConfigurationError(
                    f"{dataset.name}: unsupported format {dataset.file_format}"
                )
        return config


def _as_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    return tuple(str(v) for v in value)
