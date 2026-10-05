"""PostgreSQL connection helper shared by pipeline code."""

from __future__ import annotations

from typing import Any

import psycopg2

from edp.common.config import Settings


def connect_postgres(
    settings: Settings, *, application_name: str = "edp", connect_timeout: int = 10
) -> Any:
    """Open a connection with credentials from the environment (never hard-coded).

    ``application_name`` shows up in ``pg_stat_activity``, which makes it easy to see which
    pipeline step holds a connection.
    """
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=connect_timeout,
        application_name=application_name,
    )
