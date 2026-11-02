"""Maps the transform_method names used in config to their functions."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from rearc_quest.config.dataset_config_definition import FinalTableConfig
from rearc_quest.transforms.bls_transforms import bls_data, bls_lookup, bls_series
from rearc_quest.transforms.final_tables import PYSPARK_METHODS, load_sql
from rearc_quest.transforms.population import population

TRANSFORM_METHODS = {
    "bls_data": bls_data,
    "bls_lookup": bls_lookup,
    "bls_series": bls_series,
    "population": population,
}


def build_final(spark: SparkSession, config: FinalTableConfig, deps: dict[str, DataFrame], language: str) -> DataFrame:
    """Build a final table in one language. SQL reads the same table names as the PySpark version."""
    if language == "sql":
        return spark.sql(load_sql(config.transform_method))
    if language == "pyspark":
        return PYSPARK_METHODS[config.transform_method](deps)
    raise ValueError(f"Unknown language: {language}")
