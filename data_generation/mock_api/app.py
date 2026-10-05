"""Mock exchange-rate REST API.

Self-contained on purpose: the Docker image copies only this package. It serves the seed
file written by ``data_generation.generate files`` and behaves like a small real API:

* ``X-API-Key`` authentication
* page-based pagination (``page``, ``page_size``, ``has_more``)
* incremental reads (``updated_since`` returns rows with ``updated_at >= updated_since``;
  ``>=`` means a boundary row may be read twice, which upserts handle safely)
* optional failure injection (``MOCK_API_FAIL_EVERY_N``) to exercise client retries

Run: ``uvicorn --factory mock_api.app:create_app``
"""

from __future__ import annotations

import json
import os
import secrets
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response

MAX_PAGE_SIZE = 1000


def _load_rates(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(
            f"Exchange-rate seed file not found: {path}. Run: "
            "docker compose run --rm app python -m data_generation.generate files"
        )
    rows = json.loads(path.read_text(encoding="utf-8"))
    for row in rows:
        row["_updated"] = datetime.fromisoformat(row["updated_at"])
    rows.sort(key=lambda r: (r["_updated"], r["rate_date"], r["quote_currency"]))
    return rows


def create_app(
    data_file: str | None = None,
    api_key: str | None = None,
    fail_every_n: int | None = None,
) -> FastAPI:
    """Build the app. Arguments default to the ``EXCHANGE_RATES_FILE``, ``API_KEY`` and
    ``MOCK_API_FAIL_EVERY_N`` environment variables."""
    key = api_key if api_key is not None else os.environ.get("API_KEY", "")
    if not key:
        raise RuntimeError("API_KEY must be set; the mock API refuses to start unauthenticated")
    path = Path(data_file or os.environ.get("EXCHANGE_RATES_FILE", "/data/exchange_rates.json"))
    fail_n = (
        fail_every_n
        if fail_every_n is not None
        else int(os.environ.get("MOCK_API_FAIL_EVERY_N", 0))
    )
    rates = _load_rates(path)

    app = FastAPI(title="Mock Exchange Rate API", version="1.0")
    counter = {"requests": 0}
    lock = threading.Lock()

    def require_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not secrets.compare_digest(x_api_key, key):
            raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")

    def maybe_fail(response: Response) -> None:
        if not fail_n:
            return
        with lock:
            counter["requests"] += 1
            should_fail = counter["requests"] % fail_n == 0
        if should_fail:
            raise HTTPException(
                status_code=503, detail="Injected failure", headers={"Retry-After": "1"}
            )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "rows": len(rates)}

    @app.get("/api/v1/exchange-rates", dependencies=[Depends(require_key), Depends(maybe_fail)])
    def exchange_rates(
        updated_since: Annotated[datetime | None, Query()] = None,
        currency: Annotated[str | None, Query(min_length=3, max_length=3)] = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = 500,
    ) -> dict[str, Any]:
        selected = rates
        if updated_since is not None:
            since = updated_since if updated_since.tzinfo else updated_since.replace(tzinfo=UTC)
            selected = [r for r in selected if r["_updated"] >= since]
        if currency is not None:
            selected = [r for r in selected if r["quote_currency"] == currency.upper()]
        start = (page - 1) * page_size
        data = [
            {k: v for k, v in r.items() if k != "_updated"}
            for r in selected[start : start + page_size]
        ]
        return {
            "data": data,
            "page": page,
            "page_size": page_size,
            "total": len(selected),
            "has_more": start + page_size < len(selected),
        }

    return app
