"""Transform method for population records from the DataUSA API."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rearc_quest.config.dataset_config_definition import DatasetConfig
from rearc_quest.utils import spark_utils as su

_RECORD_SCHEMA = "`Nation ID` STRING, `Nation` STRING, `Year` INT, `Population` DOUBLE"


def population(raw: DataFrame, dependencies: dict[str, DataFrame], dataset: DatasetConfig) -> DataFrame:
    """The API returns population as a decimal such as 316128839.0, so it is read as DOUBLE first."""
    record = F.from_json(F.col(su.RAW_LINE), _RECORD_SCHEMA)
    parsed = (
        raw.withColumn("nation_id", record.getField("Nation ID"))
        .withColumn("nation", record.getField("Nation"))
        .withColumn("year", record.getField("Year"))
        .withColumn("population", record.getField("Population").cast("bigint"))
    )
    return su.select_with_metadata(su.deduplicate(parsed, dataset.keys), dataset.columns)
