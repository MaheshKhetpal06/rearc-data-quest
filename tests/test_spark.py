"""Spark function tests. Run in CI (Java) or with Databricks Connect; skipped otherwise."""

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from rearc_quest.config.dataset_config_definition import (  # noqa: E402
    get_dataset,
)
from rearc_quest.quality import checks  # noqa: E402
from rearc_quest.transforms.bls_transforms import bls_data, bls_lookup, bls_series  # noqa: E402
from rearc_quest.transforms.population import population  # noqa: E402
from rearc_quest.utils import spark_utils as su  # noqa: E402

META = "P1"


def raw_frame(spark, lines, source="file"):
    rows = [(line, "https://src/f", source, META, f"h{i}", None, None) for i, line in enumerate(lines)]
    return spark.createDataFrame(
        rows,
        f"{su.RAW_LINE} string, {su.SOURCE_URI} string, {su.FILE_NAME} string, {su.EXECUTION_PARTITION} string, "
        f"{su.RAW_DATA_HASH} string, {su.INGESTED_AT} timestamp, {su.PROCESSED_AT} timestamp",
    )


def test_drop_blank_and_header_lines_handles_padding_and_case(spark):
    df = spark.createDataFrame([("series_id        \tyear",), ("",), ("PRS1\t2000",)], "raw_line string")
    assert [r.raw_line for r in su.drop_blank_and_header_lines(df, "series_id").collect()] == ["PRS1\t2000"]
    capitalised = spark.createDataFrame([("Seasonal_code\tSeasonal_text",), ("S\tx",)], "raw_line string")
    assert su.drop_blank_and_header_lines(capitalised, "seasonal_code").count() == 1


def test_split_delimited_trims_and_keeps_trailing_empty_field(spark):
    df = spark.createDataFrame([("PRS1     \t  1.5\t",)], "raw_line string")
    row = su.split_delimited(df, ["a", "b", "c"]).first()
    assert (row.a, row.b, row.c) == ("PRS1", "1.5", "")


def test_explode_json_records_gives_one_line_per_record(spark):
    df = spark.createDataFrame([('{"data": [{"Year": 2013}, {"Year": 2014}]}',)], "raw_line string")
    lines = [r.raw_line for r in su.explode_json_records(df).collect()]
    assert len(lines) == 2
    assert "2013" in lines[0]


def test_add_raw_metadata_reads_partition_from_path(spark):
    path = "/Volumes/c/s/v/raw/d/_EXECUTION_PARTITION=2026-11-01T000000Z/pr.series"
    df = spark.createDataFrame([("x", path)], "raw_line string, _file_path string")
    row = su.add_raw_metadata(df, "https://bls/pr/", True).first()
    assert row[su.EXECUTION_PARTITION] == "2026-11-01T000000Z"
    assert row[su.FILE_NAME] == "pr.series"
    assert row[su.SOURCE_URI] == "https://bls/pr/pr.series"
    api = su.add_raw_metadata(df, "https://api/data?x=1", False).first()
    assert api[su.SOURCE_URI] == "https://api/data?x=1"
    assert len(row[su.RAW_DATA_HASH]) == 64
    assert row[su.INGESTED_AT] is not None


def test_active_snapshot_keeps_current_version_only_and_drops_removed(spark):
    raw = spark.createDataFrame(
        [("a", "f1", "P1"), ("b", "f1", "P2"), ("c", "f2", "P1")],
        f"raw_line string, {su.FILE_NAME} string, {su.EXECUTION_PARTITION} string",
    )
    manifest = spark.createDataFrame(
        [("f1", "P2", "active"), ("f2", "P1", "removed")],
        "file_name string, execution_partition string, status string",
    )
    assert [r.raw_line for r in su.active_snapshot(raw, manifest).collect()] == ["b"]


def test_try_cast_turns_bad_values_into_null(spark):
    df = spark.createDataFrame([("1.5",), ("oops",)], "v string")
    values = [r.v for r in su.try_cast_columns(df, {"v": "DECIMAL(12,3)"}).collect()]
    assert values[1] is None
    assert float(values[0]) == 1.5


def test_empty_strings_to_null(spark):
    df = spark.createDataFrame([("",)], "v string")
    assert su.empty_strings_to_null(df, ["v"]).first().v is None


def test_deduplicate_prefers_latest_partition(spark):
    df = spark.createDataFrame(
        [("k", "old", "P1", "h1"), ("k", "new", "P2", "h2")],
        f"key string, v string, {su.EXECUTION_PARTITION} string, {su.RAW_DATA_HASH} string",
    )
    assert [r.v for r in su.deduplicate(df, ["key"]).collect()] == ["new"]


def test_bls_data_transform_parses_types_and_metadata(spark):
    raw = raw_frame(spark, ["PRS1             \t2000\tQ01\t         1.500\t"])
    row = bls_data(raw, {}, get_dataset("bls_data")).first()
    assert (row.series_id, row.year, row.period, float(row.value), row.footnote_codes) == (
        "PRS1",
        2000,
        "Q01",
        1.5,
        None,
    )
    assert all(column in row.asDict() for column in su.METADATA_COLUMNS)
    assert row[su.PROCESSED_AT] is not None


def _lookup(spark, name, line):
    return bls_lookup(raw_frame(spark, [line]), {}, get_dataset(name))


def _series_deps(spark):
    return {
        "transform_bls_sector": _lookup(spark, "bls_sector", "3000\tManufacturing\t0\tT\t4"),
        "transform_bls_measure": _lookup(spark, "bls_measure", "03\tHours worked\t0\tT\t13"),
        "transform_bls_duration": _lookup(spark, "bls_duration", "2\t% Change from previous quarter\t0\tT\t2"),
        "transform_bls_class": _lookup(spark, "bls_class", "6\tAll workers\t0\tT\t1"),
    }


def test_bls_series_builds_readable_label(spark):
    line = "PRS30006032      \t3000\t6\t03\t2\tS\t-\t\t1987\tQ02\t2026\tQ02"
    row = bls_series(raw_frame(spark, [line]), _series_deps(spark), get_dataset("bls_series")).first()
    assert row.series_label == "Manufacturing - Hours worked - % Change from previous quarter (All workers)"


def test_population_transform_converts_decimal_population(spark):
    line = '{"Nation ID": "01000US", "Nation": "United States", "Year": 2013, "Population": 316128839.0}'
    row = population(raw_frame(spark, [line]), {}, get_dataset("population")).first()
    assert (row.nation_id, row.nation, row.year, row.population) == (
        "01000US",
        "United States",
        2013,
        316128839,
    )


def test_add_final_metadata_stamps_one_row_per_answer_row(spark):
    answer = spark.createDataFrame([(1,), (2,)], "x int")
    source = spark.createDataFrame(
        [("uri2", "f2", "P1", None), ("uri1", "f1", "P2", None), ("uri1", "f1", "P2", None)],
        f"{su.SOURCE_URI} string, {su.FILE_NAME} string, {su.EXECUTION_PARTITION} string, {su.INGESTED_AT} timestamp",
    )
    rows = su.add_final_metadata(answer, [source]).collect()
    assert len(rows) == 2
    assert {r[su.EXECUTION_PARTITION] for r in rows} == {"P2"}
    assert {r[su.FILE_NAME] for r in rows} == {"f1, f2"}
    assert {r[su.SOURCE_URI] for r in rows} == {"uri1, uri2"}
    assert rows[0][su.RAW_DATA_HASH] != rows[1][su.RAW_DATA_HASH]


def test_quality_checks_find_problems(spark):
    df = spark.createDataFrame([("a",), ("a",), (None,)], "k string")
    assert checks.check_duplicate_keys(df, "t", ("k",)).failing_rows == 1
    assert checks.check_null_keys(df, "t", ("k",)).failing_rows == 1
    assert checks.check_not_empty(df, "t").passed
    assert not checks.check_not_empty(df.filter(F.lit(False)), "t").passed
