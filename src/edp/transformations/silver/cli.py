"""Silver command line interface.

    python -m edp.transformations.silver load                        # load all datasets into Silver
    python -m edp.transformations.silver load --dataset orders customers

Exit codes: 0 = ok, 1 = a load failed.
"""

from __future__ import annotations

import argparse
import logging
import sys

from edp.common.config import get_settings, resolve_path
from edp.common.logging import setup_logging
from edp.common.paths import LakeLayout
from edp.common.spark import build_spark_session
from edp.ingestion.state import SqliteStateStore
from edp.transformations.silver.loader import SilverLoader
from edp.transformations.silver.schemas import SILVER_DATASET_SPECS

logger = logging.getLogger("edp.silver.cli")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m edp.transformations.silver", description=__doc__
    )
    parser.add_argument("command", choices=("load",))
    parser.add_argument("--dataset", nargs="+", metavar="NAME")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level)

    available = sorted(SILVER_DATASET_SPECS.keys())
    target_datasets = available
    if args.dataset:
        unknown = sorted(set(args.dataset) - set(available))
        if unknown:
            logger.error("Unknown dataset(s) %s; available: %s", unknown, available)
            return 1
        target_datasets = args.dataset

    spark = build_spark_session(settings, app_name=f"silver-{args.command}")
    layout = LakeLayout(settings)
    state = SqliteStateStore(resolve_path(settings.state_db_path))

    try:
        loader = SilverLoader(spark, layout, state=state)
        failed: list[str] = []

        print(f"{'dataset':<18}{'processed':>12}{'valid':>12}{'quarantined':>14}")
        for ds in target_datasets:
            try:
                res = loader.load_dataset(ds)
                if res is None:
                    print(f"{ds:<18}{'SKIPPED (no bronze table)':>38}")
                else:
                    print(
                        f"{ds:<18}{res.rows_processed:>12,}{res.rows_valid:>12,}{res.rows_quarantined:>14,}"
                    )
            except Exception as exc:
                failed.append(ds)
                print(f"{ds:<18}{'FAILED':>12}  {exc}")

        return 1 if failed else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
