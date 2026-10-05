"""Static reference data for the synthetic company."""

from __future__ import annotations

from typing import NamedTuple


class Country(NamedTuple):
    code: str  # ISO 3166-1 alpha-2
    iso3: str
    name: str
    region: str
    currency: str
    weight: float  # share of customers
    faker_locale: str


COUNTRIES: tuple[Country, ...] = (
    Country("US", "USA", "United States", "North America", "USD", 30, "en_US"),
    Country("CA", "CAN", "Canada", "North America", "CAD", 8, "en_CA"),
    Country("GB", "GBR", "United Kingdom", "Europe", "GBP", 10, "en_GB"),
    Country("DE", "DEU", "Germany", "Europe", "EUR", 10, "de_DE"),
    Country("FR", "FRA", "France", "Europe", "EUR", 7, "fr_FR"),
    Country("ES", "ESP", "Spain", "Europe", "EUR", 4, "es_ES"),
    Country("IT", "ITA", "Italy", "Europe", "EUR", 4, "it_IT"),
    Country("NL", "NLD", "Netherlands", "Europe", "EUR", 3, "nl_NL"),
    Country("BE", "BEL", "Belgium", "Europe", "EUR", 1.5, "nl_BE"),
    Country("IE", "IRL", "Ireland", "Europe", "EUR", 1.5, "en_IE"),
    Country("AT", "AUT", "Austria", "Europe", "EUR", 1.5, "de_AT"),
    Country("PT", "PRT", "Portugal", "Europe", "EUR", 1, "pt_PT"),
    Country("FI", "FIN", "Finland", "Europe", "EUR", 1, "fi_FI"),
    Country("LU", "LUX", "Luxembourg", "Europe", "EUR", 0.5, "fr_FR"),
    Country("CH", "CHE", "Switzerland", "Europe", "CHF", 2, "de_CH"),
    Country("SE", "SWE", "Sweden", "Europe", "SEK", 2, "sv_SE"),
    Country("AU", "AUS", "Australia", "Oceania", "AUD", 5, "en_AU"),
    Country("IN", "IND", "India", "Asia", "INR", 6, "en_IN"),
    Country("JP", "JPN", "Japan", "Asia", "JPY", 3, "ja_JP"),
    Country("BR", "BRA", "Brazil", "South America", "BRL", 3, "pt_BR"),
)

COUNTRY_BY_CODE: dict[str, Country] = {c.code: c for c in COUNTRIES}

# USD is the base: rate = units of currency per 1 USD.
BASE_CURRENCY = "USD"
BASE_RATES: dict[str, float] = {
    "USD": 1.0,
    "EUR": 0.92,
    "GBP": 0.79,
    "CAD": 1.36,
    "AUD": 1.52,
    "INR": 83.0,
    "JPY": 150.0,
    "CHF": 0.88,
    "SEK": 10.5,
    "BRL": 5.0,
}
DAILY_VOLATILITY: dict[str, float] = {"JPY": 0.004, "BRL": 0.005, "INR": 0.0015, "USD": 0.0}
DEFAULT_VOLATILITY = 0.003

SEGMENTS = ("consumer", "small_business", "enterprise")
SEGMENT_WEIGHTS = (75, 20, 5)

PAYMENT_METHODS = ("card", "paypal", "bank_transfer", "apple_pay", "gift_card")
PAYMENT_METHOD_WEIGHTS = (55, 20, 10, 12, 3)

# category -> (price range in USD, product nouns)
PRODUCT_CATALOG: dict[str, tuple[tuple[float, float], tuple[str, ...]]] = {
    "Electronics": (
        (15, 1500),
        ("Headphones", "Speaker", "Monitor", "Keyboard", "Webcam", "Charger", "Router", "Tablet"),
    ),
    "Home & Kitchen": (
        (8, 400),
        ("Blender", "Kettle", "Cookware Set", "Vacuum", "Lamp", "Knife Set", "Air Fryer"),
    ),
    "Clothing": (
        (10, 180),
        ("Jacket", "Hoodie", "Jeans", "Sneakers", "T-Shirt", "Backpack", "Scarf"),
    ),
    "Sports": (
        (9, 350),
        ("Yoga Mat", "Dumbbell Set", "Running Shoes", "Bike Helmet", "Water Bottle", "Tent"),
    ),
    "Books": ((5, 60), ("Novel", "Cookbook", "Biography", "Atlas", "Workbook", "Comic Collection")),
    "Beauty": ((6, 120), ("Moisturizer", "Perfume", "Hair Dryer", "Serum", "Shaving Kit")),
    "Toys": ((7, 150), ("Building Set", "Board Game", "Puzzle", "Remote Car", "Plush Toy")),
    "Office": ((4, 500), ("Desk Chair", "Notebook Pack", "Standing Desk", "Printer", "Planner")),
}
PRODUCT_ADJECTIVES = (
    "Classic",
    "Smart",
    "Compact",
    "Premium",
    "Eco",
    "Ultra",
    "Essential",
    "Deluxe",
    "Travel",
    "Studio",
)
PRODUCT_EDITIONS = ("Pro", "Plus", "Lite", "Max", "Mini", "2.0", "Classic")

REVIEW_COMMENTS = (
    "Exactly what I needed.",
    "Good value for the price.",
    "Arrived late but works well.",
    "Quality is lower than expected.",
    "Would buy again.",
    "Stopped working after a month.",
    "Great design and easy to use.",
    "Does the job, nothing special.",
    "Fantastic, highly recommended!",
    "Packaging was damaged but the product is fine.",
    "Not as described.",
    "Perfect gift.",
)
