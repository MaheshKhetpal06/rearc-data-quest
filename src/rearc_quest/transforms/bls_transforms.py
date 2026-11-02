"""Transform methods for BLS files. Each takes active raw rows and returns a clean DataFrame."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rearc_quest.config.dataset_config_definition import DatasetConfig
from rearc_quest.utils import spark_utils as su


def _parse_tsv(raw: DataFrame, dataset: DatasetConfig) -> DataFrame:
    columns = list(dataset.columns)
    parsed = su.split_delimited(raw, columns)
    parsed = su.empty_strings_to_null(parsed, columns)
    parsed = su.try_cast_columns(parsed, dataset.columns)
    return su.deduplicate(parsed, dataset.keys)


def bls_lookup(raw: DataFrame, dependencies: dict[str, DataFrame], dataset: DatasetConfig) -> DataFrame:
    return su.select_with_metadata(_parse_tsv(raw, dataset), dataset.columns)


def bls_data(raw: DataFrame, dependencies: dict[str, DataFrame], dataset: DatasetConfig) -> DataFrame:
    return su.select_with_metadata(_parse_tsv(raw, dataset), dataset.columns)


def build_series_label(sector: str, measure: str, duration: str, class_text: str) -> F.Column:
    """For example: Manufacturing - Hours worked - % Change from previous quarter (All workers)."""
    description = F.concat_ws(" - ", F.col(sector), F.col(measure), F.col(duration))
    return F.concat(description, F.lit(" ("), F.coalesce(F.col(class_text), F.lit("unknown class")), F.lit(")"))


def bls_series(raw: DataFrame, dependencies: dict[str, DataFrame], dataset: DatasetConfig) -> DataFrame:
    """Series with lookup text. Left joins keep a series whose code is new; expectations report it."""
    series = _parse_tsv(raw, dataset)
    lookups = (
        ("transform_bls_sector", "sector_code", "sector_name"),
        ("transform_bls_measure", "measure_code", "measure_text"),
        ("transform_bls_duration", "duration_code", "duration_text"),
        ("transform_bls_class", "class_code", "class_text"),
    )
    for table, key, text in lookups:
        series = series.join(dependencies[table].select(key, text), on=key, how="left")
    labelled = series.withColumn(
        "series_label", build_series_label("sector_name", "measure_text", "duration_text", "class_text")
    )
    columns = [*dataset.columns, "sector_name", "measure_text", "duration_text", "class_text", "series_label"]
    return su.select_with_metadata(labelled, columns)
