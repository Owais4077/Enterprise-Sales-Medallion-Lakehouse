"""Deterministic synthetic data generators.

Everything is driven by ``GenConfig`` (seed, volumes, defect rates) so a run is
reproducible. Money is computed in integer cents and only converted to ``Decimal`` at the
edge, which avoids floating-point artefacts such as 0.1 + 0.2 != 0.3.

Deliberate defects are injected at configurable rates and counted in ``Dataset.defects``,
so later phases can check that data-quality rules catch what was actually injected.
"""

from __future__ import annotations

import math
import random
import re
import unicodedata
import uuid
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from faker import Faker

from data_generation.reference import (
    BASE_CURRENCY,
    BASE_RATES,
    COUNTRIES,
    COUNTRY_BY_CODE,
    DAILY_VOLATILITY,
    DEFAULT_VOLATILITY,
    PAYMENT_METHOD_WEIGHTS,
    PAYMENT_METHODS,
    PRODUCT_ADJECTIVES,
    PRODUCT_CATALOG,
    PRODUCT_EDITIONS,
    REVIEW_COMMENTS,
    SEGMENT_WEIGHTS,
    SEGMENTS,
)

Row = dict[str, Any]

ORPHAN_ID_BASE = 900_000  # review customer ids at/above this match no real customer


@dataclass(frozen=True)
class GenConfig:
    """Typed view of ``config/datagen.yaml``."""

    seed: int
    start: date
    end: date
    history_start: date
    customers: int
    products: int
    orders: int
    reviews: int
    defect_rates: dict[str, float] = field(default_factory=dict)
    fx_correction_rate: float = 0.0

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> GenConfig:
        window, volumes = raw["window"], raw["volumes"]
        cfg = cls(
            seed=int(raw["seed"]),
            start=date.fromisoformat(str(window["start_date"])),
            end=date.fromisoformat(str(window["end_date"])),
            history_start=date.fromisoformat(str(window["customer_history_start"])),
            customers=int(volumes["customers"]),
            products=int(volumes["products"]),
            orders=int(volumes["orders"]),
            reviews=int(volumes["reviews"]),
            defect_rates={k: float(v) for k, v in raw.get("defects", {}).items()},
            fx_correction_rate=float(raw.get("exchange_rates", {}).get("correction_rate", 0.0)),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.history_start <= self.start < self.end:
            raise ValueError("Require customer_history_start <= start_date < end_date")
        for name in ("customers", "products", "orders", "reviews"):
            if getattr(self, name) < 1:
                raise ValueError(f"volumes.{name} must be >= 1")
        for name, rate in {**self.defect_rates, "fx": self.fx_correction_rate}.items():
            if not 0.0 <= rate <= 1.0:
                raise ValueError(f"Rate {name} must be between 0 and 1, got {rate}")

    @property
    def end_ts(self) -> datetime:
        return datetime.combine(self.end, time(23, 59, 59), tzinfo=UTC)

    def rate(self, name: str) -> float:
        return self.defect_rates.get(name, 0.0)


@dataclass
class Dataset:
    """All generated tables plus a tally of injected defects."""

    countries: list[Row]
    exchange_rates: list[Row]
    customers: list[Row]
    products: list[Row]
    orders: list[Row]
    order_items: list[Row]
    payments: list[Row]
    reviews: list[Row]
    defects: Counter[str]


def money(cents: int) -> Decimal:
    """Convert integer cents to a 2-decimal ``Decimal``."""
    return Decimal(cents).scaleb(-2)


def _cents(amount: Decimal) -> int:
    return int(amount.scaleb(2))


def _perturb(rng: random.Random, cents: int) -> int:
    """Shift an amount by 1.00-50.00 without ever making it negative, so a "mismatch" defect
    stays distinguishable from a "negative amount" defect."""
    delta = rng.randint(100, 5000)
    return cents + delta if rng.random() < 0.5 or cents - delta <= 0 else cents - delta


def _daterange(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _random_ts(rng: random.Random, first: date, last: date) -> datetime:
    """Random UTC timestamp between two dates (inclusive)."""
    day = first + timedelta(days=rng.randint(0, (last - first).days))
    return datetime.combine(day, time(0), tzinfo=UTC) + timedelta(seconds=rng.randrange(86400))


def _slug(text: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", ascii_text.lower())


def build_fakers(seed: int) -> dict[str, Faker]:
    """One seeded Faker per locale, so names look right for each customer's country."""
    fakers: dict[str, Faker] = {}
    for index, locale in enumerate(sorted({c.faker_locale for c in COUNTRIES})):
        faker = Faker(locale)
        faker.seed_instance(seed + index)
        fakers[locale] = faker
    return fakers


# --------------------------------------------------------------------------- reference


def generate_countries() -> list[Row]:
    return [
        {
            "country_code": c.code,
            "iso3": c.iso3,
            "country_name": c.name,
            "region": c.region,
            "currency_code": c.currency,
        }
        for c in COUNTRIES
    ]


def generate_exchange_rates(cfg: GenConfig, defects: Counter[str]) -> list[Row]:
    """Daily rates (units of currency per 1 USD) as a bounded random walk per currency.

    A small share of rates is "restated" two days later with a slightly different value and a
    later ``updated_at``, which gives the incremental API ingestion real updates to pick up.
    """
    days = _daterange(cfg.start, cfg.end)
    rows: list[Row] = []
    for currency in sorted(BASE_RATES):
        rng = random.Random(f"{cfg.seed}-fx-{currency}")
        volatility = DAILY_VOLATILITY.get(currency, DEFAULT_VOLATILITY)
        level = BASE_RATES[currency]
        for day in days:
            if volatility:
                level *= math.exp(rng.gauss(0, volatility))
            value = round(level, 6)
            updated_at = datetime.combine(day, time(18), tzinfo=UTC)
            if currency != BASE_CURRENCY and rng.random() < cfg.fx_correction_rate:
                value = round(value * (1 + rng.uniform(-0.01, 0.01)), 6)
                updated_at += timedelta(days=2)
                defects["exchange_rate_correction"] += 1
            rows.append(
                {
                    "rate_date": day,
                    "base_currency": BASE_CURRENCY,
                    "quote_currency": currency,
                    "rate": Decimal(f"{value:.6f}"),
                    "updated_at": updated_at,
                }
            )
    rows.sort(key=lambda r: (r["rate_date"], r["quote_currency"]))
    return rows


# ------------------------------------------------------------------------- operational


def generate_customers(
    cfg: GenConfig, fakers: dict[str, Faker], defects: Counter[str]
) -> list[Row]:
    rng = random.Random(cfg.seed + 1)
    weights = [c.weight for c in COUNTRIES]
    base: list[Row] = []
    for n in range(1, cfg.customers + 1):
        country = rng.choices(COUNTRIES, weights)[0]
        faker = fakers[country.faker_locale]
        first, last = faker.first_name()[:100], faker.last_name()[:100]
        created_at = _random_ts(rng, cfg.history_start, cfg.end)
        updated_at = created_at
        if rng.random() < 0.15:  # some profiles were edited after sign-up
            span = (cfg.end_ts - created_at).total_seconds()
            updated_at = created_at + timedelta(seconds=rng.uniform(0, span))
        city: str | None = faker.city()[:120]
        phone: str | None = faker.phone_number()[:40]
        if rng.random() < cfg.rate("customer_null_city_rate"):
            city = None
        if rng.random() < cfg.rate("customer_null_phone_rate"):
            phone = None
        code = country.code
        if rng.random() < cfg.rate("customer_messy_country_rate"):
            code = code.lower()
        base.append(
            {
                "first_name": first,
                "last_name": last,
                "email": f"{_slug(first) or 'customer'}.{_slug(last) or 'user'}{n}@example.org",
                "phone": phone,
                "country_code": code,
                "city": city,
                "segment": rng.choices(SEGMENTS, SEGMENT_WEIGHTS)[0],
                "signup_date": created_at.date(),
                "is_active": rng.random() < 0.92,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    # The same person registering again with different e-mail casing: passes the UNIQUE
    # constraint but is a duplicate in business terms.
    customers = list(base)
    duplicates = round(cfg.rate("customer_duplicate_rate") * len(base))
    for original in rng.sample(base, duplicates):
        created_at = min(original["created_at"] + timedelta(days=rng.randint(1, 90)), cfg.end_ts)
        customers.append(
            {
                **original,
                "email": original["email"].upper(),
                "signup_date": created_at.date(),
                "is_active": True,
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
        defects["customer_duplicate"] += 1

    # Counted from the final rows (duplicates inherit their original's gaps), so the tally is
    # exactly what a data-quality check can find.
    defects["customer_null_city"] = sum(c["city"] is None for c in customers)
    defects["customer_null_phone"] = sum(c["phone"] is None for c in customers)
    defects["customer_messy_country"] = sum(
        c["country_code"] != c["country_code"].upper() for c in customers
    )

    customers.sort(key=lambda c: c["created_at"])
    for customer_id, customer in enumerate(customers, start=1):
        customer["customer_id"] = customer_id
    return customers


def generate_products(cfg: GenConfig, defects: Counter[str]) -> list[Row]:
    rng = random.Random(cfg.seed + 2)
    categories = list(PRODUCT_CATALOG)
    products: list[Row] = []
    for product_id in range(1, cfg.products + 1):
        category = categories[(product_id - 1) % len(categories)]
        (low, high), nouns = PRODUCT_CATALOG[category]
        price = low * (high / low) ** rng.random()  # log-uniform: many cheap, few expensive
        price_cents = max(99, round(price) * 100 - 1)
        cost_cents = int(price_cents * rng.uniform(0.45, 0.8))
        created_at = _random_ts(rng, cfg.history_start - timedelta(days=180), cfg.history_start)
        updated_at = created_at
        if rng.random() < 0.2:  # price/description changed later
            updated_at = _random_ts(rng, created_at.date(), cfg.end)
        label = category
        if rng.random() < cfg.rate("product_messy_category_rate"):
            label = category.upper() if rng.random() < 0.5 else category.lower()
            defects["product_messy_category"] += 1
        products.append(
            {
                "product_id": product_id,
                "sku": f"SKU-{category[:3].upper()}-{product_id:04d}",
                "product_name": (
                    f"{rng.choice(PRODUCT_ADJECTIVES)} {rng.choice(nouns)} "
                    f"{rng.choice(PRODUCT_EDITIONS)}"
                ),
                "category": label,
                "unit_price": money(price_cents),
                "unit_cost": money(cost_cents),
                "currency": BASE_CURRENCY,
                "is_active": rng.random() < 0.95,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )
    return products


def _day_weights(cfg: GenConfig) -> tuple[list[date], list[float]]:
    """Order volume by day: growth over time, weekend lift, Nov/Dec peak."""
    days = _daterange(cfg.start, cfg.end)
    last = max(len(days) - 1, 1)
    season = {11: 1.4, 12: 1.7, 1: 0.9}
    weights = [
        (1 + 0.6 * i / last) * (1.15 if d.weekday() >= 5 else 1.0) * season.get(d.month, 1.0)
        for i, d in enumerate(days)
    ]
    return days, weights


_STATUS_BY_AGE = (
    (45, {"delivered": 87, "cancelled": 7, "returned": 6}),
    (10, {"delivered": 60, "shipped": 25, "paid": 6, "cancelled": 6, "returned": 3}),
    (-1, {"pending": 25, "paid": 35, "shipped": 30, "cancelled": 5, "delivered": 5}),
)
_STATUS_DELAY_DAYS = {
    "delivered": (2, 8),
    "shipped": (1, 3),
    "paid": (0, 1),
    "pending": (0, 0),
    "cancelled": (0, 2),
    "returned": (8, 30),
}


def _pick_status(rng: random.Random, age_days: int) -> str:
    for min_age, weights in _STATUS_BY_AGE:
        if age_days > min_age:
            return rng.choices(list(weights), list(weights.values()))[0]
    raise AssertionError("unreachable: last bucket matches every age")


def generate_orders(
    cfg: GenConfig,
    customers: list[Row],
    products: list[Row],
    exchange_rates: list[Row],
    defects: Counter[str],
) -> tuple[list[Row], list[Row], list[Row]]:
    """Generate orders, their line items and payments (in that tuple order)."""
    rng = random.Random(cfg.seed + 3)
    end_ts = cfg.end_ts
    rate_lookup = {(r["rate_date"], r["quote_currency"]): float(r["rate"]) for r in exchange_rates}
    usd_cents = [_cents(p["unit_price"]) for p in products]
    created_list = [c["created_at"] for c in customers]  # customers are sorted by created_at

    days, day_weights = _day_weights(cfg)
    drafts: list[dict[str, Any]] = []
    for day in rng.choices(days, day_weights, k=cfg.orders):
        ts = datetime.combine(day, time(0), tzinfo=UTC) + timedelta(seconds=rng.randrange(86400))
        eligible = bisect_right(created_list, ts)
        if eligible == 0:  # nobody existed yet: move the order to the first customer's signup
            eligible, ts = 1, created_list[0]
        # Skewed pick: earlier customers order more, giving a realistic long tail for CLV.
        customer = customers[min(int(eligible * rng.random() ** 1.5), eligible - 1)]
        country = customer["country_code"].upper()
        currency = COUNTRY_BY_CODE[country].currency
        shipping = country if rng.random() < 0.92 else rng.choice(COUNTRIES).code
        rate = rate_lookup[(min(ts.date(), cfg.end), currency)]

        lines = []
        for product_index in rng.sample(
            range(len(products)), rng.choices(range(1, 7), (18, 25, 25, 17, 10, 5))[0]
        ):
            quantity = rng.choices((1, 2, 3, 4), (70, 18, 8, 4))[0]
            discount = rng.choices((0, 5, 10, 15, 20), (60, 15, 12, 8, 5))[0]
            unit_cents = max(1, round(usd_cents[product_index] * rate))
            line_total = (quantity * unit_cents * (100 - discount) + 50) // 100
            lines.append((product_index, quantity, discount, unit_cents, line_total))

        drafts.append(
            {
                "ts": ts,
                "customer_id": customer["customer_id"],
                "currency": currency,
                "shipping": shipping,
                "status": _pick_status(rng, (end_ts - ts).days),
                "lines": lines,
                "true_total": sum(line[4] for line in lines),
            }
        )

    drafts.sort(key=lambda d: d["ts"])
    orders: list[Row] = []
    order_items: list[Row] = []
    payments: list[Row] = []
    for order_id, draft in enumerate(drafts, start=1):
        ts, status, true_total = draft["ts"], draft["status"], draft["true_total"]
        low, high = _STATUS_DELAY_DAYS[status]
        updated_at = min(
            ts + timedelta(days=rng.randint(low, high), seconds=rng.randrange(3600)), end_ts
        )

        order_date, total = ts, true_total
        if rng.random() < cfg.rate("order_future_date_rate"):
            order_date = ts + timedelta(days=rng.randint(1, 60))
            defects["order_future_date"] += 1
        if rng.random() < cfg.rate("order_negative_total_rate"):
            total = -true_total
            defects["order_negative_total"] += 1
        elif rng.random() < cfg.rate("order_total_mismatch_rate"):
            total = _perturb(rng, true_total)
            defects["order_total_mismatch"] += 1

        orders.append(
            {
                "order_id": order_id,
                "customer_id": draft["customer_id"],
                "order_date": order_date,
                "status": status,
                "currency": draft["currency"],
                "shipping_country": draft["shipping"],
                "total_amount": money(total),
                "created_at": ts,
                "updated_at": updated_at,
            }
        )
        for product_index, quantity, discount, unit_cents, _ in draft["lines"]:
            order_items.append(
                {
                    "order_item_id": len(order_items) + 1,
                    "order_id": order_id,
                    "product_id": products[product_index]["product_id"],
                    "quantity": quantity,
                    "unit_price": money(unit_cents),
                    "discount_pct": Decimal(f"{discount}.00"),
                    "created_at": ts,
                    "updated_at": updated_at,
                }
            )

        payment = _make_payment(rng, cfg, order_id, draft, updated_at, len(payments) + 1, defects)
        if payment:
            payments.append(payment)
    return orders, order_items, payments


def _make_payment(
    rng: random.Random,
    cfg: GenConfig,
    order_id: int,
    draft: dict[str, Any],
    order_updated_at: datetime,
    payment_id: int,
    defects: Counter[str],
) -> Row | None:
    status = draft["status"]
    if status in ("pending", "cancelled"):
        if rng.random() >= 0.4:
            return None  # many abandoned orders never reach the payment step
        payment_status = "pending" if status == "pending" else "failed"
    else:
        payment_status = "refunded" if status == "returned" else "completed"

    end_ts = cfg.end_ts
    ts = draft["ts"]
    amount = draft["true_total"]
    created_at = min(ts + timedelta(seconds=rng.randint(1, 15)), end_ts)
    paid_at = None
    updated_at = created_at
    if payment_status in ("completed", "refunded"):
        paid_at = max(created_at, min(ts + timedelta(seconds=rng.randint(20, 1800)), end_ts))
        updated_at = paid_at if payment_status == "completed" else max(order_updated_at, paid_at)
        if payment_status == "completed":
            if rng.random() < cfg.rate("payment_negative_rate"):
                amount = -amount
                defects["payment_negative"] += 1
            elif rng.random() < cfg.rate("payment_amount_mismatch_rate"):
                amount = _perturb(rng, amount)
                defects["payment_amount_mismatch"] += 1

    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_method": rng.choices(PAYMENT_METHODS, PAYMENT_METHOD_WEIGHTS)[0],
        "payment_status": payment_status,
        "amount": money(amount),
        "currency": draft["currency"],
        "paid_at": paid_at,
        "created_at": created_at,
        "updated_at": updated_at,
    }


# ------------------------------------------------------------------------------ files


def generate_reviews(
    cfg: GenConfig, orders: list[Row], order_items: list[Row], defects: Counter[str]
) -> list[Row]:
    """Product reviews for the JSON source. Unlike Postgres, files have no constraints, so
    orphan customers, out-of-range ratings and repeated review ids can appear."""
    rng = random.Random(cfg.seed + 4)
    delivered = [o for o in orders if o["status"] == "delivered"]
    items_by_order: dict[int, list[Row]] = defaultdict(list)
    for item in order_items:
        items_by_order[item["order_id"]].append(item)

    reviews: list[Row] = []
    for _ in range(cfg.reviews):
        order = rng.choice(delivered)
        item = rng.choice(items_by_order[order["order_id"]])
        reviewed_at = min(
            order["created_at"] + timedelta(days=rng.randint(3, 30), seconds=rng.randrange(86400)),
            cfg.end_ts,
        )
        customer_id = order["customer_id"]
        rating = rng.choices((1, 2, 3, 4, 5), (5, 5, 12, 28, 50))[0]
        if rng.random() < cfg.rate("review_orphan_customer_rate"):
            customer_id = ORPHAN_ID_BASE + rng.randint(0, 99_999)
        if rng.random() < cfg.rate("review_invalid_rating_rate"):
            rating = rng.choice((0, 6, -1, 10))
        reviews.append(
            {
                "review_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "customer_id": customer_id,
                "product_id": item["product_id"],
                "order_id": order["order_id"],
                "rating": rating,
                "comment": rng.choice(REVIEW_COMMENTS),
                "helpful_votes": rng.choices((0, 1, 2, 5, 12), (50, 25, 12, 8, 5))[0],
                "review_date": reviewed_at.isoformat(),
            }
        )

    for original in rng.sample(reviews, round(cfg.rate("review_duplicate_rate") * len(reviews))):
        reviews.append(dict(original))
        defects["review_duplicate"] += 1
    # Counted from the final rows (duplicates inherit their original's defects).
    defects["review_orphan_customer"] = sum(r["customer_id"] >= ORPHAN_ID_BASE for r in reviews)
    defects["review_invalid_rating"] = sum(not 1 <= r["rating"] <= 5 for r in reviews)
    reviews.sort(key=lambda r: r["review_date"])
    return reviews


# ---------------------------------------------------------------------------- facade


def generate_dataset(cfg: GenConfig) -> Dataset:
    """Generate every table. Same config => identical output."""
    defects: Counter[str] = Counter()
    fakers = build_fakers(cfg.seed)
    exchange_rates = generate_exchange_rates(cfg, defects)
    customers = generate_customers(cfg, fakers, defects)
    products = generate_products(cfg, defects)
    orders, order_items, payments = generate_orders(
        cfg, customers, products, exchange_rates, defects
    )
    reviews = generate_reviews(cfg, orders, order_items, defects)
    return Dataset(
        countries=generate_countries(),
        exchange_rates=exchange_rates,
        customers=customers,
        products=products,
        orders=orders,
        order_items=order_items,
        payments=payments,
        reviews=reviews,
        defects=defects,
    )
