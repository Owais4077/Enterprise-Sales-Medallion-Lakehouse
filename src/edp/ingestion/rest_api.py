"""Incremental REST API extractor.

* Authenticates with an ``X-API-Key`` header (the key comes from the environment).
* Follows pagination (``page`` / ``has_more``) until the last page.
* Sends ``updated_since=<watermark>``. The API treats that as ``>=``, so rows at exactly the
  watermark are returned again on the next run. That small overlap is deliberate (an API
  cannot be bounded the way our own database can) and harmless: Silver recognises a repeated
  key + ``updated_at`` as "already processed".
* Each HTTP request is retried with backoff on timeouts, connection errors, 429 and 5xx, and
  honours ``Retry-After``. A 401/403 or any other 4xx fails immediately.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

from edp.ingestion.base import ExtractionContext, Extractor
from edp.ingestion.config import ApiDatasetConfig
from edp.ingestion.errors import (
    ConfigurationError,
    IngestionError,
    InvalidResponseError,
    RetryableError,
    SourceAuthError,
)
from edp.ingestion.models import ExtractionStats
from edp.ingestion.retry import RetryPolicy, call_with_retry

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
API_KEY_HEADER = "X-API-Key"


def _parse_timestamp(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


class RestApiExtractor(Extractor):
    source_system = "rest_api"

    def __init__(
        self,
        *,
        client: httpx.Client,
        api: ApiDatasetConfig,
        api_key: str,
        page_size: int,
        retry: RetryPolicy,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key:
            raise ConfigurationError("API_KEY is not set; refusing to call the API unauthenticated")
        self.dataset = api.name
        self._client = client
        self._api = api
        self._api_key = api_key
        self._page_size = page_size
        self._retry = retry
        self._sleep = sleep

    def close(self) -> None:
        self._client.close()

    def _get_page(self, params: dict[str, Any]) -> dict[str, Any]:
        def request() -> dict[str, Any]:
            response = self._client.get(
                self._api.endpoint, params=params, headers={API_KEY_HEADER: self._api_key}
            )
            status = response.status_code
            if status in RETRYABLE_STATUS:
                retry_after = response.headers.get("Retry-After", "")
                raise RetryableError(
                    f"HTTP {status} from {self._api.endpoint}",
                    retry_after=float(retry_after) if retry_after.isdigit() else None,
                )
            if status in (401, 403):
                raise SourceAuthError(f"HTTP {status}: API key rejected by {self._api.endpoint}")
            if status >= 400:
                raise IngestionError(
                    f"HTTP {status} from {self._api.endpoint}: {response.text[:200]}"
                )
            try:
                body = response.json()
            except ValueError as exc:
                raise InvalidResponseError("Response is not valid JSON") from exc
            if not isinstance(body, dict) or not isinstance(body.get("data"), list):
                raise InvalidResponseError("Response must be an object with a 'data' list")
            return body

        return call_with_retry(
            request,
            self._retry,
            retry_on=(RetryableError, httpx.TransportError),
            description=f"GET {self._api.endpoint} {params}",
            sleep=self._sleep,
            retry_after=lambda exc: getattr(exc, "retry_after", None),
        )

    def extract(self, ctx: ExtractionContext) -> ExtractionStats:
        stats = ExtractionStats()
        newest: datetime | None = None
        page = 1
        while True:
            params: dict[str, Any] = {"page": page, "page_size": self._page_size}
            if ctx.since is not None:
                params["updated_since"] = ctx.since
            body = self._get_page(params)
            rows = body["data"]
            for row in rows:
                if self._api.watermark_field not in row:
                    raise InvalidResponseError(
                        f"Row is missing watermark field '{self._api.watermark_field}': {row}"
                    )
                stamp = _parse_timestamp(row[self._api.watermark_field])
                newest = stamp if newest is None or stamp > newest else newest
            ctx.sink.write_rows(rows)
            stats.records_extracted += len(rows)
            if not body.get("has_more"):
                break
            page += 1
        if newest is not None:
            stats.next_watermark = newest.isoformat()
        logger.info(
            "[rest_api.%s] since=%s -> %d rows over %d page(s), new watermark %s",
            self.dataset, ctx.since, stats.records_extracted, page, stats.next_watermark,
        )  # fmt: skip
        return stats
