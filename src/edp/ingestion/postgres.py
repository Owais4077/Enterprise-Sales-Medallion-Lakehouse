"""Incremental PostgreSQL extractor.

Reads the window ``(stored_watermark, upper]`` where ``upper = db_now - lag``:

* The lower bound is exclusive: rows already read are never read again.
* The upper bound is computed on the *database* clock (no clock-skew between machines) and
  lags behind "now" so that a transaction which stamped ``updated_at`` a moment ago but has not
  committed yet cannot fall into a gap between two windows.
* The new watermark is ``upper`` itself, even when the window was empty, so idle runs do not
  create ever-growing windows.
* Everything runs in one ``REPEATABLE READ`` read-only transaction, so ``upper`` and the data
  come from the same snapshot.

Only opening the connection is retried. A failure while streaming rows fails the run (the
runner discards the partial batch and leaves the watermark alone); the orchestrator retries
the whole task, which is safe because nothing was committed.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from edp.ingestion.base import ExtractionContext, Extractor
from edp.ingestion.config import TableConfig
from edp.ingestion.models import ExtractionStats
from edp.ingestion.retry import RetryPolicy, call_with_retry

logger = logging.getLogger(__name__)


class PostgresExtractor(Extractor):
    source_system = "postgres"

    def __init__(
        self,
        *,
        connect: Callable[[], Any],
        table: TableConfig,
        schema: str,
        batch_size: int,
        lag_seconds: float,
        retry: RetryPolicy,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.dataset = table.name
        self._connect = connect
        self._table = table
        self._schema = schema
        self._batch_size = batch_size
        self._lag_seconds = lag_seconds
        self._retry = retry
        self._sleep = sleep

    def build_query(self, has_lower_bound: bool) -> sql.Composed:
        """``SELECT *`` on purpose: new source columns flow through to Bronze, which keeps raw
        data raw. Identifiers are composed with ``psycopg2.sql`` (never string-formatted)."""
        watermark = sql.Identifier(self._table.watermark_column)
        conditions = [sql.SQL("{} <= %(upper)s").format(watermark)]
        if has_lower_bound:
            conditions.append(sql.SQL("{} > %(since)s").format(watermark))
        order = sql.SQL(", ").join(
            [watermark, *(sql.Identifier(col) for col in self._table.primary_key)]
        )
        return sql.SQL("SELECT * FROM {} WHERE {} ORDER BY {}").format(
            sql.Identifier(self._schema, self._table.name),
            sql.SQL(" AND ").join(conditions),
            order,
        )

    def extract(self, ctx: ExtractionContext) -> ExtractionStats:
        conn = call_with_retry(
            self._connect,
            self._retry,
            retry_on=(psycopg2.OperationalError,),
            description=f"connect to PostgreSQL for {self.dataset}",
            sleep=self._sleep,
        )
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn.cursor() as cur:
                cur.execute("SELECT now() - make_interval(secs => %s)", (self._lag_seconds,))
                upper: datetime = cur.fetchone()[0]

            params: dict[str, Any] = {"upper": upper}
            if ctx.since is not None:
                params["since"] = datetime.fromisoformat(ctx.since)

            stats = ExtractionStats(next_watermark=upper.isoformat())
            with conn.cursor(name=f"edp_{uuid.uuid4().hex}", cursor_factory=RealDictCursor) as cur:
                cur.execute(self.build_query(ctx.since is not None), params)
                while rows := cur.fetchmany(self._batch_size):
                    ctx.sink.write_rows(rows)
                    stats.records_extracted += len(rows)
            conn.rollback()  # read-only: just end the snapshot
            logger.info(
                "[postgres.%s] window (%s, %s] -> %d rows",
                self.dataset, ctx.since, upper.isoformat(), stats.records_extracted,
            )  # fmt: skip
            return stats
        finally:
            conn.close()
