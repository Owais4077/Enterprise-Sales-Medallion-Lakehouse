import pytest

psycopg2 = pytest.importorskip("psycopg2")

from data_generation.pg_loader import connect  # noqa: E402
from edp.common.config import Settings  # noqa: E402


@pytest.fixture(scope="session")
def pg_connection():
    """Live connection to the source database, or a skip if it is not running."""
    try:
        conn = connect(Settings())
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable ({exc}); run `docker compose up -d postgres`")
    yield conn
    conn.close()


@pytest.fixture
def cur(pg_connection):
    """Cursor inside a transaction that is always rolled back: tests never change data."""
    cursor = pg_connection.cursor()
    yield cursor
    pg_connection.rollback()
    cursor.close()
