"""Simple pyspark.sql checks, run after the pipeline as a second line behind its expectations."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from rearc_quest.config.dataset_config_definition import DATASETS, FINAL_TABLES


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
    for table, keys in targets:
        df = spark.read.table(table)
        results += [
            check_not_empty(df, table),
            check_null_keys(df, table, keys),
            check_duplicate_keys(df, table, keys),
        ]
    return results
