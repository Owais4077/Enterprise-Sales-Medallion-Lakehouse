"""Command-line entry point for generating the synthetic sources.

python -m data_generation.generate files      # CSV, JSON, API seed file, manifest
python -m data_generation.generate postgres   # load the PostgreSQL source database
python -m data_generation.generate all
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from data_generation import file_writers
from data_generation.generators import GenConfig, generate_dataset
from edp.common.config import PROJECT_ROOT, get_settings, load_yaml_config
from edp.common.logging import setup_logging

logger = logging.getLogger("data_generation")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m data_generation.generate")
    parser.add_argument("target", choices=("files", "postgres", "all"))
    parser.add_argument("--data-dir", default="data", help="output root (default: ./data)")
    parser.add_argument(
        "--force", action="store_true", help="allow loading PostgreSQL when ENVIRONMENT=aws"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = get_settings()
    setup_logging(settings.log_level)

    load_postgres = args.target in ("postgres", "all")
    if load_postgres and settings.environment == "aws" and not args.force:
        logger.error("Refusing to TRUNCATE and reload tables with ENVIRONMENT=aws (use --force)")
        return 2

    data_dir = Path(args.data_dir)
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir

    cfg = GenConfig.from_dict(load_yaml_config("datagen"))
    logger.info("Generating dataset (seed=%s, orders=%s)", cfg.seed, f"{cfg.orders:,}")
    dataset = generate_dataset(cfg)

    if args.target in ("files", "all"):
        file_writers.write_countries_csv(
            dataset.countries, data_dir / "raw" / "countries" / "countries.csv"
        )
        review_files = file_writers.write_reviews_json(
            dataset.reviews, data_dir / "raw" / "reviews"
        )
        file_writers.write_exchange_rates_json(
            dataset.exchange_rates, data_dir / "api_source" / "exchange_rates.json"
        )
        manifest_path = file_writers.write_manifest(
            file_writers.build_manifest(cfg, dataset), data_dir / "generation_manifest.json"
        )
        logger.info("Wrote %d review files; manifest at %s", len(review_files), manifest_path)

    if load_postgres:
        from data_generation import pg_loader  # imported here so 'files' needs no database driver

        try:
            conn = pg_loader.connect(settings)
        except Exception:
            logger.exception(
                "Cannot connect to PostgreSQL at %s:%s (is `docker compose up -d postgres` up?)",
                settings.postgres_host,
                settings.postgres_port,
            )
            return 1
        try:
            pg_loader.load_dataset(conn, dataset)
        except Exception:
            return 1
        finally:
            conn.close()

    logger.info("Injected defects: %s", dict(sorted(dataset.defects.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
