import json

import httpx
import pytest
from fastapi.testclient import TestClient

from data_generation import file_writers
from data_generation.generators import GenConfig, generate_dataset
from data_generation.mock_api.app import create_app
from edp.ingestion.config import ApiDatasetConfig
from edp.ingestion.errors import (
    ConfigurationError,
    InvalidResponseError,
    SourceAuthError,
)
from edp.ingestion.rest_api import RestApiExtractor
from edp.ingestion.retry import RetryPolicy
from edp.ingestion.runner import IngestionRunner
from edp.ingestion.state import SqliteStateStore
from tests.unit.test_generators import RAW_CONFIG

KEY = "test-key"
API = ApiDatasetConfig("exchange_rates", "/api/v1/exchange-rates", "updated_at", ("rate_date",))
FAST = RetryPolicy(max_attempts=4, base_delay=0.0, max_delay=0.0, jitter=0.0)


@pytest.fixture(scope="module")
def seed(tmp_path_factory):
    dataset = generate_dataset(GenConfig.from_dict(RAW_CONFIG))
    path = tmp_path_factory.mktemp("seed") / "fx.json"
    file_writers.write_exchange_rates_json(dataset.exchange_rates, path)
    return path, len(dataset.exchange_rates)


def make_extractor(client, key=KEY, page_size=500, retry=FAST):
    return RestApiExtractor(
        client=client, api=API, api_key=key, page_size=page_size, retry=retry, sleep=lambda s: None
    )


@pytest.fixture
def runner(tmp_path):
    state = SqliteStateStore(tmp_path / "state.db")
    return IngestionRunner(state=state, landing_root=tmp_path / "landing"), state


def landed_rows(result):
    import gzip

    rows = []
    for part in sorted(result.landing_path.glob("part-*.jsonl.gz")):
        with gzip.open(part, "rt", encoding="utf-8") as handle:
            rows += [json.loads(line) for line in handle]
    return rows


def test_full_pull_follows_pagination(seed, runner):
    path, total = seed
    run, state = runner
    client = TestClient(create_app(str(path), api_key=KEY, fail_every_n=0))
    result = run.run(make_extractor(client, page_size=700))

    assert result.records_extracted == total and result.records_processed == total
    rows = landed_rows(result)
    assert len({(r["rate_date"], r["quote_currency"]) for r in rows}) == total
    assert result.watermark_to == max(r["updated_at"] for r in rows)


def test_second_run_only_rereads_the_watermark_boundary(seed, runner):
    path, total = seed
    run, state = runner
    app = create_app(str(path), api_key=KEY, fail_every_n=0)
    first = run.run(make_extractor(TestClient(app)))  # the runner closes each client after a run
    second = run.run(make_extractor(TestClient(app)))

    # `>=` semantics: only rows at exactly the watermark are re-read (a handful, not the table).
    assert 0 < second.records_extracted < 20 < total
    assert {r["updated_at"] for r in landed_rows(second)} == {first.watermark_to}
    assert second.watermark_to == first.watermark_to


def test_transient_server_errors_are_retried_transparently(seed, runner):
    path, total = seed
    run, _ = runner
    flaky = TestClient(create_app(str(path), api_key=KEY, fail_every_n=2))  # every 2nd call = 503
    result = run.run(make_extractor(flaky, page_size=1000))
    assert result.records_extracted == total


def test_persistent_outage_fails_the_run_after_retries(seed, runner):
    path, _ = seed
    run, state = runner
    down = TestClient(create_app(str(path), api_key=KEY, fail_every_n=1))  # always 503
    with pytest.raises(Exception, match="503"):
        run.run(make_extractor(down))
    assert state.get_watermark("rest_api", "exchange_rates") is None
    assert state.list_runs()[0]["status"] == "failed"


def test_wrong_api_key_fails_immediately_without_retrying(seed, runner):
    path, _ = seed
    run, state = runner
    hits = []
    app = create_app(str(path), api_key=KEY, fail_every_n=0)
    client = TestClient(app)
    original_get = client.get
    client.get = lambda *a, **k: (hits.append(1), original_get(*a, **k))[1]

    with pytest.raises(SourceAuthError):
        run.run(make_extractor(client, key="wrong"))
    assert len(hits) == 1  # no retry storm against a rejected key
    assert state.list_runs()[0]["error_type"] == "SourceAuthError"


def test_missing_api_key_refuses_to_start():
    with pytest.raises(ConfigurationError):
        make_extractor(httpx.Client(), key="")


def _mock_client(handler):
    return httpx.Client(base_url="http://api", transport=httpx.MockTransport(handler))


def test_connection_errors_and_retry_after_are_honoured(runner):
    run, _ = runner
    calls = {"n": 0}
    sleeps: list[float] = []

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("refused")
        if calls["n"] == 2:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json={"data": [{"updated_at": "2025-01-01T00:00:00+00:00"}]})

    policy = RetryPolicy(max_attempts=4, base_delay=1.0, max_delay=30.0, jitter=0.0)
    extractor = RestApiExtractor(
        client=_mock_client(handler),
        api=API,
        api_key=KEY,
        page_size=10,
        retry=policy,
        sleep=sleeps.append,
    )
    result = run.run(extractor)
    assert result.records_extracted == 1 and calls["n"] == 3
    assert sleeps == [1.0, 7.0]  # backoff, then the server's Retry-After


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="<html>oops</html>"),
        httpx.Response(200, json=["not", "an", "object"]),
        httpx.Response(200, json={"rows": []}),
        httpx.Response(200, json={"data": [{"no_watermark": 1}]}),
    ],
)
def test_malformed_responses_fail_loudly_without_retry(runner, response):
    run, state = runner
    calls = []
    extractor = make_extractor(_mock_client(lambda req: (calls.append(1), response)[1]))
    with pytest.raises(InvalidResponseError):
        run.run(extractor)
    assert len(calls) == 1


def test_other_client_errors_are_not_retried(runner):
    run, _ = runner
    calls = []
    extractor = make_extractor(_mock_client(lambda req: (calls.append(1), httpx.Response(404))[1]))
    with pytest.raises(Exception, match="404"):
        run.run(extractor)
    assert len(calls) == 1
