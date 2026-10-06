"""Data Quality CLI command line interface.

    python -m edp.quality run --layer silver                     # run all Silver quality suites
    python -m edp.quality run --layer gold                       # run all Gold quality suites

Exit codes: 0 = all quality suites passed, 1 = any quality check failed.
"""

from __future__ import annotations

import argparse
import logging
import sys

from delta.tables import DeltaTable

from edp.common.config import get_settings
from edp.common.logging import setup_logging
from edp.common.paths import LakeLayout, Layer
from edp.common.spark import build_spark_session
from edp.quality.runner import DataQualityRunner
from edp.quality.suites import get_gold_suites, get_silver_suites

logger = logging.getLogger("edp.quality.cli")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m edp.quality", description=__doc__)
    parser.add_argument("command", choices=("run",))
    parser.add_argument("--layer", choices=("silver", "gold"), default="silver")
    parser.add_argument("--dataset", nargs="+", metavar="NAME")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level)

    spark = build_spark_session(settings, app_name=f"quality-{args.layer}")
    layout = LakeLayout(settings)

    suites_dict = get_silver_suites() if args.layer == "silver" else get_gold_suites()

    if args.dataset:
        suites_dict = {name: suites_dict[name] for name in args.dataset if name in suites_dict}

    runner = DataQualityRunner()
    failed_suites = 0

    target_layer_enum = Layer.SILVER if args.layer == "silver" else Layer.GOLD

    try:
        print(f"\nEvaluating Data Quality for Layer [{args.layer.upper()}]")
        print("=" * 70)

        for name, suite in suites_dict.items():
            path = layout.table_path(target_layer_enum, name)
            if not DeltaTable.isDeltaTable(spark, path):
                print(f"{'SKIP':<6} {name:<20} Table does not exist at {path}")
                continue

            df = spark.read.format("delta").load(path)
            res = runner.run_suite(df, suite)

            status_str = "PASS" if res.passed else "FAIL"
            if not res.passed:
                failed_suites += 1

            print(f"{status_str:<6} {name:<20} {res.passed_rules}/{res.total_rules} rules passed")
            for r in res.rule_results:
                symbol = "✓" if r.passed else "✗"
                print(f"       {symbol} {r.rule_name:<45} {r.detail}")

        print("-" * 70)
        if failed_suites > 0:
            print(f"RESULT: FAILED ({failed_suites} suite(s) with errors)")
            return 1

        print("RESULT: PASSED (All quality checks succeeded)")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
