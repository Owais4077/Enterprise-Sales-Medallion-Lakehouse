import copy

import pytest

from edp.common.config import Settings, load_yaml_config
from edp.ingestion.config import IngestionConfig
from edp.ingestion.errors import ConfigurationError
from edp.ingestion.factory import build_extractors
from edp.ingestion.files import FileExtractor
from edp.ingestion.postgres import PostgresExtractor
from edp.ingestion.rest_api import RestApiExtractor


@pytest.fixture(scope="module")
def raw_config():
    return load_yaml_config("pipeline")


@pytest.fixture(scope="module")
def config(raw_config):
    return IngestionConfig.from_dict(raw_config)


@pytest.fixture
def settings():
    return Settings(_env_file=None, api_key="k", postgres_password="p")


def test_real_pipeline_yaml_parses(config):
    assert set(config.tables) == {"customers", "products", "orders", "order_items", "payments"}
    assert config.tables["orders"].watermark_column == "updated_at"
    assert config.api_datasets["exchange_rates"].primary_key == ("rate_date", "quote_currency")
    assert {d.file_format for d in config.file_datasets.values()} == {"csv", "json"}
    assert config.retry.max_attempts == 5


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c["ingestion"].pop("retry"),
        lambda c: c["ingestion"].update(batch_size=0),
        lambda c: c["sources"]["files"]["reviews"].update(format="xml"),
        lambda c: c["sources"]["postgres"]["tables"]["orders"].pop("watermark_column"),
        lambda c: c["ingestion"]["retry"].update(max_attempts=0),
    ],
)
def test_invalid_config_raises_configuration_error(raw_config, mutate):
    broken = copy.deepcopy(raw_config)
    try:
        mutate(broken)
        IngestionConfig.from_dict(broken)
    except ValueError as exc:  # RetryPolicy validation surfaces as ValueError -> wrapped
        pytest.fail(f"unwrapped ValueError: {exc}")
    except ConfigurationError:
        return
    pytest.fail("expected ConfigurationError")


def test_builds_one_extractor_per_dataset(settings, config):
    extractors = build_extractors(settings, config)
    kinds = [type(e) for e in extractors]
    assert kinds.count(PostgresExtractor) == 5
    assert kinds.count(RestApiExtractor) == 1
    assert kinds.count(FileExtractor) == 2
    for e in extractors:
        e.close()


def test_source_and_dataset_filters(settings, config):
    only_pg = build_extractors(settings, config, sources=["postgres"], datasets=["orders"])
    assert [(e.source_system, e.dataset) for e in only_pg] == [("postgres", "orders")]
    files = build_extractors(settings, config, sources=["files"])
    assert {e.dataset for e in files} == {"countries", "reviews"}


def test_unknown_names_are_errors_not_silent_noops(settings, config):
    with pytest.raises(ConfigurationError, match="nope"):
        build_extractors(settings, config, datasets=["nope"])
    with pytest.raises(ConfigurationError, match="Unknown source"):
        build_extractors(settings, config, sources=["kafka"])
    with pytest.raises(ConfigurationError, match="orders"):  # orders is not a files dataset
        build_extractors(settings, config, sources=["files"], datasets=["orders"])


def test_missing_api_key_is_caught_at_build_time(config):
    with pytest.raises(ConfigurationError, match="API_KEY"):
        build_extractors(Settings(_env_file=None, api_key=""), config, sources=["api"])
