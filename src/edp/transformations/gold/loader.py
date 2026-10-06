"""Gold loader: transforms Silver Delta tables into Gold star schema & KPI Delta tables."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from edp.common.paths import LakeLayout, Layer
from edp.transformations.gold.dimensions import (
    build_dim_country,
    build_dim_customer,
    build_dim_date,
    build_dim_product,
)
from edp.transformations.gold.facts import build_fact_sales
from edp.transformations.gold.kpis import build_kpi_monthly_sales

logger = logging.getLogger(__name__)


class GoldLoadError(Exception):
    """Raised when a Gold table cannot be generated safely."""


@dataclass(frozen=True)
class GoldLoadResult:
    table_name: str
    rows_loaded: int


class GoldLoader:
    def __init__(
        self,
        spark: SparkSession,
        layout: LakeLayout,
    ) -> None:
        self._spark = spark
        self._layout = layout

    def silver_table_path(self, dataset: str) -> str:
        return self._layout.table_path(Layer.SILVER, dataset)

    def gold_table_path(self, table_name: str) -> str:
        return self._layout.table_path(Layer.GOLD, table_name)

    def silver_exists(self, dataset: str) -> bool:
        return DeltaTable.isDeltaTable(self._spark, self.silver_table_path(dataset))

    def read_silver(self, dataset: str):
        if not self.silver_exists(dataset):
            return None
        return self._spark.read.format("delta").load(self.silver_table_path(dataset))

    def _write_gold(self, table_name: str, df) -> GoldLoadResult:
        cnt = df.count()
        path = self.gold_table_path(table_name)
        logger.info("[%s] Writing %d rows to Gold table at %s", table_name, cnt, path)
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
        return GoldLoadResult(table_name, cnt)

    def build_gold_tables(self) -> list[GoldLoadResult]:
        results: list[GoldLoadResult] = []

        # 1. Date Dimension
        dim_date_df = build_dim_date(self._spark)
        results.append(self._write_gold("dim_date", dim_date_df))

        # 2. Customer Dimension
        cust_silver = self.read_silver("customers")
        if cust_silver is not None:
            results.append(self._write_gold("dim_customer", build_dim_customer(cust_silver)))

        # 3. Product Dimension
        prod_silver = self.read_silver("products")
        if prod_silver is not None:
            results.append(self._write_gold("dim_product", build_dim_product(prod_silver)))

        # 4. Country Dimension
        cntry_silver = self.read_silver("countries")
        if cntry_silver is not None:
            results.append(self._write_gold("dim_country", build_dim_country(cntry_silver)))

        # 5. Sales Fact Table
        orders_silver = self.read_silver("orders")
        items_silver = self.read_silver("order_items")
        rates_silver = self.read_silver("exchange_rates")

        fact_sales_df = None
        if orders_silver is not None and items_silver is not None:
            fact_sales_df = build_fact_sales(orders_silver, items_silver, rates_silver)
            results.append(self._write_gold("fact_sales", fact_sales_df))

        # 6. KPI Aggregations
        if fact_sales_df is not None:
            kpi_df = build_kpi_monthly_sales(fact_sales_df, prod_silver)
            results.append(self._write_gold("kpi_monthly_sales", kpi_df))

        return results
