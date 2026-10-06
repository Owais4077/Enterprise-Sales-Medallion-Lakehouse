"""Spark test fixtures. Spark needs Java >= 11 and a supported Python, so these tests run in the
Docker image and are skipped (not failed) on a host without them."""

import re
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from edp.common.config import Settings
from edp.common.paths import LakeLayout
from edp.ingestion.landing import LandingBatch, sha256_file


def pytest_ignore_collect(collection_path, config):
    """Without PySpark/Delta (e.g. on the Windows host) these modules cannot even be imported,
    so they are not collected. In the Docker image both are present and the tests run."""
    try:
        import delta  # noqa: F401
        import pyspark  # noqa: F401
    except ImportError:
        return True
    return None


def pytest_collection_modifyitems(items):
    for item in items:
        if "tests/spark/" in item.nodeid.replace("\\", "/"):
            item.add_marker(pytest.mark.spark)


def _java_major() -> int | None:
    java = shutil.which("java")
    if not java:
        return None
    output = subprocess.run([java, "-version"], capture_output=True, text=True).stderr  # noqa: S603
    match = re.search(r'version "(\d+)(?:\.(\d+))?', output)
    if not match:
        return None
    major = int(match.group(1))
    return int(match.group(2)) if major == 1 else major


@pytest.fixture
def tmp_path():
    """Override pytest's tmp_path with a directory on the container's native filesystem.

    The project folder is a bind mount from the Windows host; Delta performs many small file
    operations that are ~20x slower there, which made this suite take six minutes.
    """
    with tempfile.TemporaryDirectory(prefix="edp-spark-test-") as directory:
        yield Path(directory)


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    major = _java_major()
    if major is None or major < 11:
        pytest.skip(f"Spark tests need Java >= 11 (found {major}); run them in Docker")
    from edp.common.spark import build_spark_session

    settings = Settings(_env_file=None, spark_shuffle_partitions=2, spark_driver_memory="1g")
    session = build_spark_session(
        settings,
        app_name="tests",
        extra_conf={"spark.sql.warehouse.dir": tempfile.mkdtemp(prefix="edp-warehouse-")},
    )
    yield session
    session.stop()


@pytest.fixture
def layout(tmp_path) -> LakeLayout:
    return LakeLayout(Settings(_env_file=None, local_lake_root=str(tmp_path / "lake")))


@pytest.fixture
def landing(tmp_path) -> Path:
    return tmp_path / "landing"


EXTRACTED_AT = datetime(2026, 1, 5, 10, 0, tzinfo=UTC)


def land_rows(landing, source, dataset, batch_id, rows, extracted_at=EXTRACTED_AT):
    """Create a committed row batch exactly as the Phase 3 ingestion would."""
    batch = LandingBatch(landing, source, dataset, batch_id)
    batch.write_rows(rows)
    return batch.commit({"run_id": f"run-{batch_id}", "extracted_at": extracted_at.isoformat()})


def land_files(landing, source, dataset, batch_id, files, extracted_at=EXTRACTED_AT):
    """Create a committed file batch. ``files`` maps file name -> (bytes, record_count)."""
    staging = landing.parent / f"staging-{batch_id}"
    staging.mkdir(parents=True, exist_ok=True)
    batch = LandingBatch(landing, source, dataset, batch_id)
    for name, (content, count) in files.items():
        path = staging / name
        path.write_bytes(content)
        batch.add_file(path, record_count=count, sha256=sha256_file(path))
    return batch.commit({"run_id": f"run-{batch_id}", "extracted_at": extracted_at.isoformat()})
