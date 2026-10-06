"""Gold command line interface.

    python -m edp.transformations.gold build                    # build all Gold star schema tables

Exit codes: 0 = ok, 1 = a build failed.
"""

from __future__ import annotations

import argparse
import logging
import sys

from edp.common.config import get_settings
from edp.common.logging import setup_logging
from edp.common.paths import LakeLayout
from edp.common.spark import build_spark_session
from edp.transformations.gold.loader import GoldLoader

logger = logging.getLogger("edp.gold.cli")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m edp.transformations.gold", description=__doc__)
    parser.add_argument("command", choices=("build",))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level)

    spark = build_spark_session(settings, app_name=f"gold-{args.command}")
    layout = LakeLayout(settings)

    try:
        loader = GoldLoader(spark, layout)
        results = loader.build_gold_tables()

        print(f"\n{'table name':<25}{'rows loaded':>15}")
        print("-" * 40)
        for res in results:
            print(f"{res.table_name:<25}{res.rows_loaded:>15,}")
        return 0
    except Exception as exc:
        logger.exception("Gold build FAILED")
        print(f"FAILED: {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
