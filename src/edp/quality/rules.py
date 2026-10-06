"""Data quality rule definitions."""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Any, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class RuleResult:
    rule_name: str
    passed: bool
    failed_count: int
    total_rows: int
    detail: str


class QualityRule(abc.ABC):
    @property
    @abc.abstractmethod
    def name(self) -> str: ...

    @abc.abstractmethod
    def evaluate(self, df: DataFrame) -> RuleResult: ...


class NullCheck(QualityRule):
    """Ensures a column contains 0 null values."""

    def __init__(self, column: str) -> None:
        self._column = column

    @property
    def name(self) -> str:
        return f"NullCheck({self._column})"

    def evaluate(self, df: DataFrame) -> RuleResult:
        if self._column not in df.columns:
            return RuleResult(self.name, False, 0, 0, f"Column '{self._column}' not found")
        total = df.count()
        if total == 0:
            return RuleResult(self.name, True, 0, 0, "Empty DataFrame")

        null_cnt = df.filter(F.col(self._column).isNull()).count()
        passed = null_cnt == 0
        detail = "No null values" if passed else f"Found {null_cnt} null value(s)"
        return RuleResult(self.name, passed, null_cnt, total, detail)


class RangeCheck(QualityRule):
    """Ensures numeric or date values fall within [min_val, max_val]."""

    def __init__(self, column: str, min_val: Any = None, max_val: Any = None) -> None:
        self._column = column
        self._min_val = min_val
        self._max_val = max_val

    @property
    def name(self) -> str:
        return f"RangeCheck({self._column}, min={self._min_val}, max={self._max_val})"

    def evaluate(self, df: DataFrame) -> RuleResult:
        if self._column not in df.columns:
            return RuleResult(self.name, False, 0, 0, f"Column '{self._column}' not found")
        total = df.count()
        if total == 0:
            return RuleResult(self.name, True, 0, 0, "Empty DataFrame")

        cond = F.lit(True)
        if self._min_val is not None:
            cond = cond & (F.col(self._column) >= self._min_val)
        if self._max_val is not None:
            cond = cond & (F.col(self._column) <= self._max_val)

        failed_cnt = df.filter(~cond).count()
        passed = failed_cnt == 0
        detail = "All values within range" if passed else f"Found {failed_cnt} value(s) out of range"
        return RuleResult(self.name, passed, failed_cnt, total, detail)


class SetCheck(QualityRule):
    """Ensures column values belong to a set of allowed values."""

    def __init__(self, column: str, allowed_values: Sequence[Any]) -> None:
        self._column = column
        self._allowed_values = tuple(allowed_values)

    @property
    def name(self) -> str:
        return f"SetCheck({self._column}, allowed={self._allowed_values})"

    def evaluate(self, df: DataFrame) -> RuleResult:
        if self._column not in df.columns:
            return RuleResult(self.name, False, 0, 0, f"Column '{self._column}' not found")
        total = df.count()
        if total == 0:
            return RuleResult(self.name, True, 0, 0, "Empty DataFrame")

        failed_cnt = df.filter(~F.col(self._column).isin(*self._allowed_values)).count()
        passed = failed_cnt == 0
        detail = "All values in allowed set" if passed else f"Found {failed_cnt} invalid value(s)"
        return RuleResult(self.name, passed, failed_cnt, total, detail)


class UniqueCheck(QualityRule):
    """Ensures key column combinations are unique across the dataset."""

    def __init__(self, columns: Sequence[str]) -> None:
        self._columns = tuple(columns)

    @property
    def name(self) -> str:
        return f"UniqueCheck({', '.join(self._columns)})"

    def evaluate(self, df: DataFrame) -> RuleResult:
        for col in self._columns:
            if col not in df.columns:
                return RuleResult(self.name, False, 0, 0, f"Column '{col}' not found")

        total = df.count()
        if total == 0:
            return RuleResult(self.name, True, 0, 0, "Empty DataFrame")

        distinct_cnt = df.select(*self._columns).distinct().count()
        failed_cnt = total - distinct_cnt
        passed = failed_cnt == 0
        detail = "All keys unique" if passed else f"Found {failed_cnt} duplicate key row(s)"
        return RuleResult(self.name, passed, failed_cnt, total, detail)


class ReferentialIntegrityCheck(QualityRule):
    """Ensures foreign key values in child_col exist in parent_df.parent_col."""

    def __init__(self, child_col: str, parent_df: DataFrame, parent_col: str) -> None:
        self._child_col = child_col
        self._parent_df = parent_df
        self._parent_col = parent_col

    @property
    def name(self) -> str:
        return f"ReferentialIntegrityCheck({self._child_col} -> parent.{self._parent_col})"

    def evaluate(self, df: DataFrame) -> RuleResult:
        if self._child_col not in df.columns:
            return RuleResult(self.name, False, 0, 0, f"Column '{self._child_col}' not found")
        total = df.count()
        if total == 0:
            return RuleResult(self.name, True, 0, 0, "Empty DataFrame")

        parents_slim = self._parent_df.select(F.col(self._parent_col).alias("_parent_pk")).distinct()
        orphans = df.join(parents_slim, df[self._child_col] == parents_slim._parent_pk, how="left_anti")
        failed_cnt = orphans.count()
        passed = failed_cnt == 0
        detail = "All foreign keys valid" if passed else f"Found {failed_cnt} orphan record(s)"
        return RuleResult(self.name, passed, failed_cnt, total, detail)

