import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from edp.ingestion.models import RunStatus
from edp.ingestion.state import SqliteStateStore


def test_analytics_views_execute_and_query_metrics(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = SqliteStateStore(db_path)

    # 1. Populate sample run data
    now = datetime.now(UTC)
    store.start_run(
        run_id="r1",
        pipeline_name="ingestion",
        source_system="postgres",
        dataset="orders",
        batch_id="b1",
        started_at=now,
        watermark_from=None,
    )
    store.finish_run(
        run_id="r1",
        status=RunStatus.SUCCESS,
        finished_at=now,
        records_extracted=100,
        records_processed=100,
        records_rejected=0,
        watermark_to=None,
    )

    store.start_run(
        run_id="r2",
        pipeline_name="ingestion",
        source_system="postgres",
        dataset="orders",
        batch_id="b2",
        started_at=now,
        watermark_from=None,
    )
    store.finish_run(
        run_id="r2",
        status=RunStatus.FAILED,
        finished_at=now,
        records_extracted=50,
        records_processed=0,
        records_rejected=50,
        watermark_to=None,
        error=ValueError("DB error"),
    )

    # 2. Execute SQL views DDL
    sql_path = Path(__file__).parents[2] / "sql" / "analytics" / "001_pipeline_audit_views.sql"
    assert sql_path.is_file()

    sql_script = sql_path.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(sql_script)

        # 3. Query vw_pipeline_run_summary
        cursor = conn.cursor()
        cursor.execute(
            "SELECT dataset, total_runs, successful_runs, failed_runs FROM vw_pipeline_run_summary"
        )
        summary_row = cursor.fetchone()
        assert summary_row is not None
        assert summary_row[0] == "orders"
        assert summary_row[1] == 2  # total_runs
        assert summary_row[2] == 1  # successful_runs
        assert summary_row[3] == 1  # failed_runs

        # 4. Query vw_pipeline_failed_runs
        cursor.execute("SELECT run_id, error_type FROM vw_pipeline_failed_runs")
        failed_row = cursor.fetchone()
        assert failed_row is not None
        assert failed_row[0] == "r2"
        assert failed_row[1] == "ValueError"
