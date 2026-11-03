"""Simple pyspark.sql checks, run after the pipeline as a second line behind its expectations."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rearc_quest.config.dataset_config_definition import DATASETS, FINAL_TABLES

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    table: str
    check: str
    failing_rows: int

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


def check_not_empty(df: DataFrame, table: str) -> CheckResult:
    return CheckResult(table, "not_empty", 0 if df.limit(1).count() else 1)


def check_null_keys(df: DataFrame, table: str, keys: tuple[str, ...]) -> CheckResult:
    any_null = F.lit(False)
    for key in keys:
        any_null = any_null | F.col(key).isNull()
    return CheckResult(table, "null_keys", df.filter(any_null).count())


def check_duplicate_keys(df: DataFrame, table: str, keys: tuple[str, ...]) -> CheckResult:
    return CheckResult(table, "duplicate_keys", df.groupBy(*keys).count().filter("count > 1").count())


def run_all_checks(spark: SparkSession) -> list[CheckResult]:
    targets = [(d.transform_table_name, d.keys) for d in DATASETS if d.is_modeled]
    targets += [(f.name, f.keys) for f in FINAL_TABLES]
    results = []
    log.info("Running quality checks on %d tables", len(targets))
    for table, keys in targets:
        df = spark.read.table(table)
        table_results = [
            check_not_empty(df, table),
            check_null_keys(df, table, keys),
            check_duplicate_keys(df, table, keys),
        ]
        results += table_results
        outcome = ", ".join(
            f"{r.check}={'pass' if r.passed else f'FAIL ({r.failing_rows} rows)'}" for r in table_results
        )
        if all(r.passed for r in table_results):
            log.info("Quality checks for %s (keys %s): %s", table, ", ".join(keys), outcome)
        else:
            log.error("Quality checks for %s (keys %s): %s", table, ", ".join(keys), outcome)
    return results
