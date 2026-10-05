import pytest
from pydantic import ValidationError

from edp.common.config import Settings, load_yaml_config


def test_defaults_are_local_and_secret_is_masked():
    s = Settings(_env_file=None, postgres_password="hunter2")
    assert s.storage_backend == "local"
    assert "hunter2" not in repr(s)
    assert s.postgres_password.get_secret_value() == "hunter2"


def test_jdbc_url_has_no_password():
    s = Settings(_env_file=None, postgres_password="hunter2")
    assert "hunter2" not in s.postgres_jdbc_url


def test_s3_backend_requires_bucket():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, storage_backend="s3", s3_bucket="")


def test_env_vars_override_defaults(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db.internal")
    assert Settings(_env_file=None).postgres_host == "db.internal"


def test_pipeline_yaml_loads_and_missing_file_fails():
    cfg = load_yaml_config("pipeline")
    assert "orders" in cfg["sources"]["postgres"]
    with pytest.raises(FileNotFoundError):
        load_yaml_config("does_not_exist")
