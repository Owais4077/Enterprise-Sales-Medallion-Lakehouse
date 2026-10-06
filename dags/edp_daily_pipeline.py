"""Master Daily End-to-End Lakehouse Pipeline DAG.

Orchestrates: Ingestion -> Bronze Load & Validation -> Silver MERGE & Quality
              -> Gold Star Schema & Quality.
"""

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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="edp_daily_pipeline",
        default_args=DEFAULT_ARGS,
        description="Master daily lakehouse ingestion and medallion transformations pipeline",
        schedule_interval="0 2 * * *",  # 02:00 UTC daily
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["edp", "production", "daily", "lakehouse"],
    ) as dag:

        task_ingest = BashOperator(
            task_id="ingest_all_sources",
            bash_command="python -m edp.ingestion",
        )

        task_bronze_load = BashOperator(
            task_id="load_bronze_layer",
            bash_command="python -m edp.transformations.bronze load",
        )

        task_bronze_val = BashOperator(
            task_id="validate_bronze_layer",
            bash_command="python -m edp.transformations.bronze validate",
        )

        task_silver_load = BashOperator(
            task_id="load_silver_layer",
            bash_command="python -m edp.transformations.silver load",
        )

        task_silver_dq = BashOperator(
            task_id="run_silver_data_quality",
            bash_command="python -m edp.quality run --layer silver",
        )

        task_gold_build = BashOperator(
            task_id="build_gold_star_schema",
            bash_command="python -m edp.transformations.gold build",
        )

        task_gold_dq = BashOperator(
            task_id="run_gold_data_quality",
            bash_command="python -m edp.quality run --layer gold",
        )

        # Pipeline Task Dependency Graph
        (
            task_ingest
            >> task_bronze_load
            >> task_bronze_val
            >> task_silver_load
            >> task_silver_dq
            >> task_gold_build
            >> task_gold_dq
        )
