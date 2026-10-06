"""Hourly Frequent Ingestion DAG for orders and exchange rates."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = None  # type: ignore[assignment,misc]
    BashOperator = None  # type: ignore[assignment,misc]


DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="edp_hourly_ingestion",
        default_args=DEFAULT_ARGS,
        description="Hourly frequent ingestion for high-frequency orders & exchange rates",
        schedule_interval="0 * * * *",  # top of every hour
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["edp", "production", "hourly", "ingestion"],
    ) as dag:

        task_ingest_hourly = BashOperator(
            task_id="ingest_frequent_sources",
            bash_command="python -m edp.ingestion",
        )

        task_bronze_hourly = BashOperator(
            task_id="load_bronze_frequent",
            bash_command=(
                "python -m edp.transformations.bronze load --dataset orders exchange_rates"
            ),
        )

        task_ingest_hourly >> task_bronze_hourly
