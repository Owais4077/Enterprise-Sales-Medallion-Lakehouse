import importlib.util
from pathlib import Path

import pytest

DAG_DIR = Path(__file__).parents[2] / "dags"


def test_dag_files_import_without_syntax_errors():
    dag_files = list(DAG_DIR.glob("*.py"))
    assert len(dag_files) >= 2, f"Expected at least 2 DAG files in {DAG_DIR}"

    for dag_file in dag_files:
        spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)


def test_airflow_dagbag_if_airflow_installed():
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Airflow not installed in local environment")

    dagbag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

    daily_dag = dagbag.get_dag("edp_daily_pipeline")
    assert daily_dag is not None
    assert len(daily_dag.tasks) == 7

    hourly_dag = dagbag.get_dag("edp_hourly_ingestion")
    assert hourly_dag is not None
    assert len(hourly_dag.tasks) == 2
