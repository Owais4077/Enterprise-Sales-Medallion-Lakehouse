"""Command-line entry point.

    python -m edp.ingestion                       # all sources, incremental
    python -m edp.ingestion --source postgres --dataset orders customers
    python -m edp.ingestion --full-refresh        # ignore stored watermarks / processed files

Exit codes: 0 = all succeeded, 1 = at least one failed, 3 = finished but some files rejected.
One failing dataset does not stop the others; every failure is logged and audited.
"""

from __future__ import annotations

import argparse
import logging
import sys

from edp.common.config import get_settings, load_yaml_config, resolve_path
from edp.common.logging import setup_logging
from edp.ingestion.config import IngestionConfig
from edp.ingestion.errors import ConfigurationError
from edp.ingestion.factory import SOURCE_TYPES, build_extractors
from edp.ingestion.models import RunResult, RunStatus
from edp.ingestion.runner import IngestionRunner
from edp.ingestion.state import SqliteStateStore

logger = logging.getLogger("edp.ingestion.cli")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m edp.ingestion", description=__doc__)
    parser.add_argument("--source", choices=(*SOURCE_TYPES, "all"), default="all")
    parser.add_argument("--dataset", nargs="+", metavar="NAME", help="limit to these datasets")
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--pipeline-name", default="ingestion")
    return parser.parse_args(argv)


def format_summary(results: list[RunResult], failures: list[tuple[str, str]]) -> str:
    lines = [f"{'dataset':<26}{'status':<9}{'extracted':>10}{'landed':>10}  watermark_to"]
    for r in results:
        lines.append(
            f"{r.source_system + '.' + r.dataset:<26}{r.status.value:<9}"
            f"{r.records_extracted:>10,}{r.records_processed:>10,}  {r.watermark_to or '-'}"
        )
        for rejected in r.rejected_files:
            lines.append(f"    rejected {rejected.file_name}: {rejected.reason}")
    for name, error in failures:
        lines.append(f"{name:<26}{'FAILED':<9}  {error}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level)

    try:
        config = IngestionConfig.from_dict(load_yaml_config("pipeline"))
        sources = SOURCE_TYPES if args.source == "all" else (args.source,)
        extractors = build_extractors(settings, config, sources=sources, datasets=args.dataset)
    except ConfigurationError:
        logger.exception("Configuration error")
        return 1
    if not extractors:
        logger.error("No datasets selected")
        return 1

    runner = IngestionRunner(
        state=SqliteStateStore(resolve_path(settings.state_db_path)),
        landing_root=resolve_path(settings.landing_dir),
        pipeline_name=args.pipeline_name,
        max_rows_per_file=config.max_rows_per_file,
    )
    results: list[RunResult] = []
    failures: list[tuple[str, str]] = []
    for extractor in extractors:
        try:
            results.append(runner.run(extractor, full_refresh=args.full_refresh))
        except Exception as exc:  # already logged + audited by the runner; keep going
            failures.append((f"{extractor.source_system}.{extractor.dataset}", repr(exc)))

    print(format_summary(results, failures))
    if failures:
        return 1
    return 3 if any(r.status is RunStatus.PARTIAL for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
