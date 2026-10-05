import pytest
from fastapi.testclient import TestClient

from data_generation import file_writers
from data_generation.generators import GenConfig, generate_dataset
from data_generation.mock_api.app import create_app
from tests.unit.test_generators import RAW_CONFIG

KEY = "test-key"
HEADERS = {"X-API-Key": KEY}
URL = "/api/v1/exchange-rates"


@pytest.fixture(scope="module")
def seed_rows(tmp_path_factory):
    ds = generate_dataset(GenConfig.from_dict(RAW_CONFIG))
    path = tmp_path_factory.mktemp("api") / "exchange_rates.json"
    file_writers.write_exchange_rates_json(ds.exchange_rates, path)
    return path, ds.exchange_rates


@pytest.fixture
def client(seed_rows):
    return TestClient(create_app(str(seed_rows[0]), api_key=KEY, fail_every_n=0))


def test_health_needs_no_key(client):
    assert client.get("/health").json()["status"] == "ok"


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
def test_data_endpoint_requires_valid_key(client, headers):
    assert client.get(URL, headers=headers).status_code == 401


def test_pagination_covers_every_row_exactly_once(client, seed_rows):
    total = len(seed_rows[1])
    seen, page = [], 1
    while True:
        body = client.get(URL, headers=HEADERS, params={"page": page, "page_size": 700}).json()
        seen += [(r["rate_date"], r["quote_currency"]) for r in body["data"]]
        assert body["total"] == total
        if not body["has_more"]:
            break
        page += 1
    assert len(seen) == total == len(set(seen))


def test_updated_since_returns_only_recent_rows(client, seed_rows):
    cutoff = "2024-12-01T00:00:00Z"
    body = client.get(
        URL, headers=HEADERS, params={"updated_since": cutoff, "page_size": 1000}
    ).json()
    assert 0 < body["total"] < len(seed_rows[1])
    assert all(r["updated_at"] >= "2024-12-01" for r in body["data"])


def test_updated_since_without_timezone_is_treated_as_utc(client):
    aware = client.get(URL, headers=HEADERS, params={"updated_since": "2024-12-01T00:00:00+00:00"})
    naive = client.get(URL, headers=HEADERS, params={"updated_since": "2024-12-01T00:00:00"})
    assert aware.json()["total"] == naive.json()["total"]


def test_currency_filter(client):
    body = client.get(URL, headers=HEADERS, params={"currency": "eur", "page_size": 1000}).json()
    assert body["total"] > 0
    assert {r["quote_currency"] for r in body["data"]} == {"EUR"}


@pytest.mark.parametrize(
    "params",
    [{"updated_since": "not-a-date"}, {"page_size": 5000}, {"page": 0}, {"currency": "EURO"}],
)
def test_invalid_parameters_are_rejected(client, params):
    assert client.get(URL, headers=HEADERS, params=params).status_code == 422


def test_failure_injection_returns_503_with_retry_after(seed_rows):
    flaky = TestClient(create_app(str(seed_rows[0]), api_key=KEY, fail_every_n=3))
    codes = [flaky.get(URL, headers=HEADERS).status_code for _ in range(6)]
    assert codes == [200, 200, 503, 200, 200, 503]
    assert flaky.get(URL, headers=HEADERS).status_code == 200
    failing = TestClient(create_app(str(seed_rows[0]), api_key=KEY, fail_every_n=1))
    assert failing.get(URL, headers=HEADERS).headers["Retry-After"] == "1"


def test_app_refuses_to_start_without_key_or_data(seed_rows, tmp_path):
    with pytest.raises(RuntimeError):
        create_app(str(seed_rows[0]), api_key="")
    with pytest.raises(FileNotFoundError):
        create_app(str(tmp_path / "missing.json"), api_key=KEY)
