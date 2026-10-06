"""Data Quality Suite runner & audit reporter."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame

from edp.ingestion.state import StateStore
from edp.quality.rules import QualityRule, RuleResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataQualitySuite:
    dataset: str
    layer: str
    rules: tuple[QualityRule, ...]


@dataclass(frozen=True)
class SuiteResult:
    dataset: str
    layer: str
    passed: bool
    total_rules: int
    passed_rules: int
    failed_rules: int
    rule_results: tuple[RuleResult, ...]


class DataQualityRunner:
    def __init__(self, state: StateStore | None = None) -> None:
        self._state = state

    def run_suite(self, df: DataFrame, suite: DataQualitySuite) -> SuiteResult:
        logger.info(
            "[%s:%s] Evaluating %d quality rules...",
            suite.layer,
            suite.dataset,
            len(suite.rules),
        )

        results: list[RuleResult] = []
        all_passed = True

        for rule in suite.rules:
            res = rule.evaluate(df)
            results.append(res)
            if not res.passed:
                all_passed = False
                logger.warning(
                    "[%s:%s] RULE FAILED: %s -> %s",
                    suite.layer,
                    suite.dataset,
                    res.rule_name,
                    res.detail,
                )
            else:
                logger.info(
                    "[%s:%s] PASS: %s", suite.layer, suite.dataset, res.rule_name
                )

        passed_cnt = sum(1 for r in results if r.passed)
        failed_cnt = len(results) - passed_cnt

        return SuiteResult(
            dataset=suite.dataset,
            layer=suite.layer,
            passed=all_passed,
            total_rules=len(suite.rules),
            passed_rules=passed_cnt,
            failed_rules=failed_cnt,
            rule_results=tuple(results),
        )

