"""Bronze command line.

    python -m edp.transformations.bronze load                    # load all pending batches
    python -m edp.transformations.bronze load --dataset orders
    python -m edp.transformations.bronze validate                # landing vs Bronze checks

Exit codes: 0 = ok, 1 = a load failed / a validation check failed.
"""

from __future__ import annotations

import argparse
import logging
import sys

from edp.common.config import get_settings, load_yaml_config, resolve_path
from edp.common.logging import setup_logging
from edp.common.paths import LakeLayout
from edp.common.spark import build_spark_session
from edp.ingestion.config import IngestionConfig
from edp.ingestion.state import SqliteStateStore
from edp.transformations.bronze.loader import BronzeLoader, dataset_specs
from edp.transformations.bronze.validate import validate_dataset

logger = logging.getLogger("edp.bronze.cli")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m edp.transformations.bronze", description=__doc__
    )
    parser.add_argument("command", choices=("load", "validate"))
    parser.add_argument("--dataset", nargs="+", metavar="NAME")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level)

    specs = dataset_specs(IngestionConfig.from_dict(load_yaml_config("pipeline")))
    if args.dataset:
        unknown = sorted(set(args.dataset) - set(specs))
        if unknown:
            logger.error("Unknown dataset(s) %s; available: %s", unknown, sorted(specs))
            return 1
        specs = {name: specs[name] for name in args.dataset}

    spark = build_spark_session(settings, app_name=f"bronze-{args.command}")
    layout = LakeLayout(settings)
    landing_root = resolve_path(settings.landing_dir)
    try:
        if args.command == "load":
            return _load(spark, layout, landing_root, settings, specs)
        return _validate(spark, layout, landing_root, specs)
    finally:
        spark.stop()


def _load(spark, layout, landing_root, settings, specs) -> int:
    loader = BronzeLoader(
        spark, layout, landing_root, state=SqliteStateStore(resolve_path(settings.state_db_path))
    )
    failed: list[str] = []
    print(f"{'dataset':<18}{'batches loaded':>15}{'rows loaded':>13}")
    for name, spec in specs.items():
        try:
            results = loader.load_dataset(spec)
            print(f"{name:<18}{len(results):>15}{sum(r.rows_loaded for r in results):>13,}")
        except Exception as exc:  # logged + audited in the loader; continue with other datasets
            failed.append(name)
            print(f"{name:<18}{'FAILED':>15}  {exc}")
    return 1 if failed else 0


def _validate(spark, layout, landing_root, specs) -> int:
    results = [
        r for spec in specs.values() for r in validate_dataset(spark, layout, landing_root, spec)
    ]
    for r in results:
        print(f"{'PASS' if r.passed else 'FAIL'}  {r.dataset:<14}{r.check:<28}{r.detail}")
    failed = [r for r in results if not r.passed]
    print(f"\n{len(results) - len(failed)} passed, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
