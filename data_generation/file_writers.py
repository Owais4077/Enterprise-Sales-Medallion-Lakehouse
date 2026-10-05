"""Write the file-based sources (CSV, JSON), the API seed file and the manifest."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from data_generation.generators import Dataset, GenConfig, Row

COUNTRY_COLUMNS = ("country_code", "iso3", "country_name", "region", "currency_code")


def write_countries_csv(rows: list[Row], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COUNTRY_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_reviews_json(reviews: list[Row], directory: Path) -> list[Path]:
    """One JSON-array file per review month (``reviews_YYYY-MM.json``).

    Monthly files let Bronze record ``file_name`` and skip files it has already loaded.
    Stale files from a previous run are removed first so the folder matches the dataset.
    """
    directory.mkdir(parents=True, exist_ok=True)
    for stale in directory.glob("reviews_*.json"):
        stale.unlink()
    by_month: dict[str, list[Row]] = defaultdict(list)
    for review in reviews:
        by_month[review["review_date"][:7]].append(review)
    paths = []
    for month, month_rows in sorted(by_month.items()):
        path = directory / f"reviews_{month}.json"
        path.write_text(json.dumps(month_rows, ensure_ascii=False), encoding="utf-8")
        paths.append(path)
    return paths


def write_exchange_rates_json(rows: list[Row], path: Path) -> Path:
    """Seed file served by the mock REST API (the API's "database")."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "rate_date": r["rate_date"].isoformat(),
            "base_currency": r["base_currency"],
            "quote_currency": r["quote_currency"],
            "rate": float(r["rate"]),
            "updated_at": r["updated_at"].isoformat(),
        }
        for r in rows
    ]
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def build_manifest(cfg: GenConfig, dataset: Dataset) -> dict[str, Any]:
    """Row counts and injected-defect counts: ground truth for testing data quality."""
    return {
        "seed": cfg.seed,
        "window": {"start": cfg.start.isoformat(), "end": cfg.end.isoformat()},
        "row_counts": {
            "customers": len(dataset.customers),
            "products": len(dataset.products),
            "orders": len(dataset.orders),
            "order_items": len(dataset.order_items),
            "payments": len(dataset.payments),
            "countries": len(dataset.countries),
            "exchange_rates": len(dataset.exchange_rates),
            "reviews": len(dataset.reviews),
        },
        "injected_defects": dict(sorted(dataset.defects.items())),
    }


def write_manifest(manifest: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path
