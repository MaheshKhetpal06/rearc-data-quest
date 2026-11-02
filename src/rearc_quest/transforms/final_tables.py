"""The three answers, each in Spark SQL (src/rearc_quest/sql) and in the PySpark DataFrame API.

Config selects the primary version that feeds the pipeline. The other is the documented alternative.
"""

from __future__ import annotations

from importlib import resources

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

QUARTERS = ("Q01", "Q02", "Q03", "Q04")
REPORT_SERIES_ID = "PRS30006032"
REPORT_PERIOD = "Q01"


def load_sql(transform_method: str) -> str:
    return resources.files("rearc_quest.sql").joinpath(f"{transform_method}.sql").read_text()


def population_stats(deps: dict[str, DataFrame]) -> DataFrame:
    window = deps["transform_population"].filter(F.col("year").between(2013, 2018))
    return window.agg(
        F.min("year").alias("min_year"),
        F.max("year").alias("max_year"),
        F.count("*").alias("years_included"),
        F.round(F.avg("population"), 2).alias("mean_population"),
        F.round(F.stddev_samp("population"), 2).alias("stddev_population"),
    )


def series_best_year(deps: dict[str, DataFrame]) -> DataFrame:
    """Q05 is an annual average, not a quarter, so it is excluded. Ties go to the latest year."""
    yearly = (
        deps["transform_bls_data"]
        .filter(F.col("period").isin(*QUARTERS))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("summed_value"), F.count("*").alias("quarters_in_best_year"))
    )
    ranking = Window.partitionBy("series_id").orderBy(F.col("summed_value").desc(), F.col("year").desc())
    best = yearly.withColumn("_rank", F.row_number().over(ranking)).filter("_rank = 1")
    series = deps["transform_bls_series"].select("series_id", "series_label")
    return series.join(best, on="series_id", how="left").select(
        "series_id",
        "series_label",
        F.col("year").alias("best_year"),
        "summed_value",
        "quarters_in_best_year",
        F.col("year").isNotNull().alias("has_quarterly_data"),
    )


def series_population_report(deps: dict[str, DataFrame]) -> DataFrame:
    data = deps["transform_bls_data"].filter(
        (F.col("series_id") == REPORT_SERIES_ID) & (F.col("period") == REPORT_PERIOD)
    )
    population = deps["transform_population"].select("year", "population")
    return data.join(population, on="year", how="left").select("series_id", "year", "period", "value", "population")


PYSPARK_METHODS = {
    "population_stats": population_stats,
    "series_best_year": series_best_year,
    "series_population_report": series_population_report,
}
