from collections import Counter
from datetime import date
from decimal import Decimal

import pytest

from data_generation.generators import GenConfig, generate_dataset
from data_generation.reference import COUNTRY_BY_CODE

RAW_CONFIG = {
    "seed": 7,
    "window": {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "customer_history_start": "2023-10-01",
    },
    "volumes": {"customers": 300, "products": 40, "orders": 2500, "reviews": 400},
    # High rates so every defect type reliably occurs in a small dataset.
    "defects": {
        "customer_duplicate_rate": 0.05,
        "customer_null_city_rate": 0.1,
        "customer_null_phone_rate": 0.1,
        "customer_messy_country_rate": 0.1,
        "product_messy_category_rate": 0.1,
        "order_future_date_rate": 0.02,
        "order_negative_total_rate": 0.02,
        "order_total_mismatch_rate": 0.05,
        "payment_negative_rate": 0.03,
        "payment_amount_mismatch_rate": 0.05,
        "review_orphan_customer_rate": 0.05,
        "review_invalid_rating_rate": 0.05,
        "review_duplicate_rate": 0.05,
    },
    "exchange_rates": {"correction_rate": 0.02},
}


@pytest.fixture(scope="module")
def cfg() -> GenConfig:
    return GenConfig.from_dict(RAW_CONFIG)


@pytest.fixture(scope="module")
def ds(cfg):
    return generate_dataset(cfg)


def test_generation_is_deterministic(cfg, ds):
    again = generate_dataset(cfg)
    assert again.customers == ds.customers
    assert again.orders == ds.orders
    assert again.payments == ds.payments
    assert again.reviews == ds.reviews
    assert again.defects == ds.defects


def test_different_seed_gives_different_data(ds):
    other = generate_dataset(GenConfig.from_dict({**RAW_CONFIG, "seed": 8}))
    assert other.orders != ds.orders


def test_volumes(cfg, ds):
    assert len(ds.products) == cfg.products
    assert len(ds.orders) == cfg.orders
    assert len(ds.customers) == cfg.customers + ds.defects["customer_duplicate"]
    assert len(ds.order_items) >= len(ds.orders)
    assert len(ds.reviews) == cfg.reviews + ds.defects["review_duplicate"]


def test_postgres_foreign_keys_hold(ds):
    customers = {c["customer_id"] for c in ds.customers}
    products = {p["product_id"] for p in ds.products}
    orders = {o["order_id"] for o in ds.orders}
    assert {o["customer_id"] for o in ds.orders} <= customers
    assert {i["order_id"] for i in ds.order_items} <= orders
    assert {i["product_id"] for i in ds.order_items} <= products
    assert {p["order_id"] for p in ds.payments} <= orders


def test_ids_are_unique_and_sequential(ds):
    for rows, key in (
        (ds.customers, "customer_id"),
        (ds.orders, "order_id"),
        (ds.order_items, "order_item_id"),
        (ds.payments, "payment_id"),
    ):
        assert [r[key] for r in rows] == list(range(1, len(rows) + 1))


def test_rows_satisfy_database_check_constraints(ds):
    assert all(i["quantity"] > 0 and i["unit_price"] >= 0 for i in ds.order_items)
    assert all(Decimal(0) <= i["discount_pct"] <= 100 for i in ds.order_items)
    assert len({(i["order_id"], i["product_id"]) for i in ds.order_items}) == len(ds.order_items)
    assert all(p["unit_price"] >= 0 and p["unit_cost"] >= 0 for p in ds.products)
    assert all(p["paid_at"] for p in ds.payments if p["payment_status"] == "completed")
    assert len({c["email"] for c in ds.customers}) == len(ds.customers)
    assert len({p["sku"] for p in ds.products}) == len(ds.products)


def test_updated_at_never_precedes_created_at(ds):
    for rows in (ds.customers, ds.products, ds.orders, ds.order_items, ds.payments):
        assert all(r["updated_at"] >= r["created_at"] for r in rows)


def test_money_has_two_decimal_places(ds):
    for row in ds.orders:
        assert row["total_amount"].as_tuple().exponent == -2
    for row in ds.payments:
        assert row["amount"].as_tuple().exponent == -2


def test_orders_are_placed_after_the_customer_exists(ds):
    created = {c["customer_id"]: c["created_at"] for c in ds.customers}
    assert all(o["created_at"] >= created[o["customer_id"]] for o in ds.orders)


def test_order_currency_matches_customer_country(ds):
    country = {c["customer_id"]: c["country_code"].upper() for c in ds.customers}
    assert all(
        o["currency"] == COUNTRY_BY_CODE[country[o["customer_id"]]].currency for o in ds.orders
    )


def line_total(item: dict) -> Decimal:
    cents = int(item["unit_price"].scaleb(2)) * item["quantity"]
    return Decimal((cents * (100 - int(item["discount_pct"])) + 50) // 100).scaleb(-2)


def order_sums(ds) -> dict[int, Decimal]:
    sums: dict[int, Decimal] = Counter()
    for item in ds.order_items:
        sums[item["order_id"]] += line_total(item)
    return sums


def test_clean_orders_total_equals_sum_of_items(ds):
    sums = order_sums(ds)
    off = [o for o in ds.orders if o["total_amount"] != sums[o["order_id"]]]
    expected = ds.defects["order_negative_total"] + ds.defects["order_total_mismatch"]
    assert len(off) == expected


# --- injected defects are real, observable and correctly counted -------------------


def test_defect_counts_match_observable_data(ds):
    sums = order_sums(ds)
    d = ds.defects
    assert sum(1 for o in ds.orders if o["total_amount"] < 0) == d["order_negative_total"]
    assert sum(1 for o in ds.orders if o["order_date"] > o["created_at"]) == d["order_future_date"]
    assert (
        sum(
            1
            for o in ds.orders
            if o["total_amount"] >= 0 and o["total_amount"] != sums[o["order_id"]]
        )
        == d["order_total_mismatch"]
    )
    assert sum(1 for p in ds.payments if p["amount"] < 0) == d["payment_negative"]
    assert sum(1 for c in ds.customers if c["city"] is None) == d["customer_null_city"]
    assert (
        sum(1 for c in ds.customers if c["country_code"] != c["country_code"].upper())
        == d["customer_messy_country"]
    )
    lowered = Counter(c["email"].lower() for c in ds.customers)
    assert sum(n - 1 for n in lowered.values() if n > 1) == d["customer_duplicate"]


def test_every_defect_type_occurs(ds):
    expected = {
        "customer_duplicate", "customer_null_city", "customer_messy_country",
        "product_messy_category", "order_future_date", "order_negative_total",
        "order_total_mismatch", "payment_negative", "payment_amount_mismatch",
        "review_orphan_customer", "review_invalid_rating", "review_duplicate",
        "exchange_rate_correction",
    }  # fmt: skip
    assert expected <= {name for name, count in ds.defects.items() if count > 0}


def test_review_defects(ds):
    customers = {c["customer_id"] for c in ds.customers}
    assert sum(1 for r in ds.reviews if r["customer_id"] not in customers) == (
        ds.defects["review_orphan_customer"]
    )
    assert sum(1 for r in ds.reviews if not 1 <= r["rating"] <= 5) == (
        ds.defects["review_invalid_rating"]
    )
    assert len(ds.reviews) - len({r["review_id"] for r in ds.reviews}) == (
        ds.defects["review_duplicate"]
    )


def test_exchange_rates(cfg, ds):
    days = (cfg.end - cfg.start).days + 1
    assert len(ds.exchange_rates) == days * 10
    assert all(r["rate"] == 1 for r in ds.exchange_rates if r["quote_currency"] == "USD")
    assert all(r["rate"] > 0 for r in ds.exchange_rates)
    restated = [r for r in ds.exchange_rates if r["updated_at"].date() > r["rate_date"]]
    assert len(restated) == ds.defects["exchange_rate_correction"]
    assert {r["rate_date"] for r in ds.exchange_rates} == {
        date.fromordinal(n) for n in range(cfg.start.toordinal(), cfg.end.toordinal() + 1)
    }


def test_countries_reference(ds):
    assert len(ds.countries) == len(COUNTRY_BY_CODE)
    assert {c["currency_code"] for c in ds.countries} <= {
        r["quote_currency"] for r in ds.exchange_rates
    }


# --- config validation ----------------------------------------------------------------


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c["window"].update(start_date="2025-01-01"),  # start after end
        lambda c: c["volumes"].update(orders=0),
        lambda c: c["defects"].update(order_negative_total_rate=1.5),
    ],
)
def test_invalid_config_is_rejected(mutate):
    import copy

    raw = copy.deepcopy(RAW_CONFIG)
    mutate(raw)
    with pytest.raises(ValueError):
        GenConfig.from_dict(raw)
