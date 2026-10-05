"""Build extractors from configuration, so adding a dataset is a YAML change."""

from __future__ import annotations

from collections.abc import Iterable

import httpx

from edp.common.config import Settings, resolve_path
from edp.common.db import connect_postgres
from edp.ingestion.base import Extractor
from edp.ingestion.config import IngestionConfig
from edp.ingestion.errors import ConfigurationError
from edp.ingestion.files import FileExtractor
from edp.ingestion.postgres import PostgresExtractor
from edp.ingestion.rest_api import RestApiExtractor

SOURCE_TYPES = ("postgres", "api", "files")


def build_extractors(
    settings: Settings,
    config: IngestionConfig,
    *,
    sources: Iterable[str] = SOURCE_TYPES,
    datasets: Iterable[str] | None = None,
) -> list[Extractor]:
    """Return one extractor per configured dataset, optionally filtered.

    Unknown source types or dataset names raise ``ConfigurationError`` rather than silently
    running nothing.
    """
    selected_sources = set(sources)
    unknown_sources = selected_sources - set(SOURCE_TYPES)
    if unknown_sources:
        raise ConfigurationError(f"Unknown source type(s): {sorted(unknown_sources)}")

    available: dict[str, list[str]] = {
        "postgres": list(config.tables),
        "api": list(config.api_datasets),
        "files": list(config.file_datasets),
    }
    wanted = set(datasets) if datasets else None
    if wanted:
        known = {name for kind in selected_sources for name in available[kind]}
        if wanted - known:
            raise ConfigurationError(
                f"Unknown dataset(s) {sorted(wanted - known)}; "
                f"available for sources {sorted(selected_sources)}: {sorted(known)}"
            )

    def chosen(name: str) -> bool:
        return wanted is None or name in wanted

    extractors: list[Extractor] = []
    if "postgres" in selected_sources:
        for table in config.tables.values():
            if chosen(table.name):
                extractors.append(
                    PostgresExtractor(
                        connect=lambda: connect_postgres(
                            settings, application_name="edp-ingestion"
                        ),
                        table=table,
                        schema=config.pg_schema,
                        batch_size=config.batch_size,
                        lag_seconds=config.watermark_lag_seconds,
                        retry=config.retry,
                    )
                )
    if "api" in selected_sources:
        for api in config.api_datasets.values():
            if chosen(api.name):
                client = httpx.Client(
                    base_url=settings.api_base_url, timeout=config.api_timeout_seconds
                )
                extractors.append(
                    RestApiExtractor(
                        client=client,
                        api=api,
                        api_key=settings.api_key.get_secret_value(),
                        page_size=config.api_page_size,
                        retry=config.retry,
                    )
                )
    if "files" in selected_sources:
        raw_root = resolve_path(settings.raw_data_dir)
        for dataset in config.file_datasets.values():
            if chosen(dataset.name):
                extractors.append(FileExtractor(dataset=dataset, root=raw_root))
    return extractors
