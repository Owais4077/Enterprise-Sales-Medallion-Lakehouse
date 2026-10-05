"""Configuration management.

Two kinds of configuration, deliberately kept apart:

* **Settings** (``.env`` / environment variables): secrets and per-environment values
  such as passwords and hostnames. Never committed.
* **Pipeline config** (``config/*.yaml``): structure of the pipeline such as table names,
  keys and watermark columns. Committed and reviewed like code.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_DIR = PROJECT_ROOT / "config"


class Settings(BaseSettings):
    """Environment-driven settings. Field names map to upper-case env vars."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: Literal["local", "ci", "aws"] = "local"
    log_level: str = "INFO"

    storage_backend: Literal["local", "s3"] = "local"
    local_lake_root: str = "data/lake"
    s3_bucket: str = ""
    s3_prefix: str = "edp"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "sales_oltp"
    postgres_user: str = "edp_user"
    postgres_password: SecretStr = SecretStr("")

    landing_dir: str = "data/landing"
    raw_data_dir: str = "data/raw"
    state_db_path: str = "data/state/pipeline_state.db"

    api_base_url: str = "http://localhost:8000"
    api_key: SecretStr = SecretStr("")

    @model_validator(mode="after")
    def _s3_requires_bucket(self) -> Settings:
        if self.storage_backend == "s3" and not self.s3_bucket:
            raise ValueError("S3_BUCKET must be set when STORAGE_BACKEND=s3")
        return self

    @property
    def postgres_jdbc_url(self) -> str:
        """JDBC URL for Spark. The password is passed separately, never embedded."""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


def resolve_path(value: str | Path) -> Path:
    """Resolve a configured path; relative paths are anchored at the project root."""
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


@lru_cache
def get_settings() -> Settings:
    """Return cached settings (one read of the environment per process)."""
    return Settings()


def load_yaml_config(name: str, config_dir: Path | None = None) -> dict[str, Any]:
    """Load ``config/<name>.yaml``. Fails loudly if the file is missing or empty."""
    path = (config_dir or DEFAULT_CONFIG_DIR) / f"{name}.yaml"
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a YAML mapping")
    return data
