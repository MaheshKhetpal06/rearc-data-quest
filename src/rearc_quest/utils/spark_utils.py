"""Reusable Spark building blocks. Every transform method is composed from these functions.

Apart from write_to_uc_table, nothing here writes or triggers a Spark action, so all of it is safe
inside Spark Declarative Pipeline dataset functions.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

RAW_LINE = "raw_line"
FILE_NAME = "_FILE_NAME"
SOURCE_URI = "_SOURCE_URI"
EXECUTION_PARTITION = "_EXECUTION_PARTITION"
RAW_DATA_HASH = "_RAW_DATA_HASH"
INGESTED_AT = "_INGESTED_AT"
PROCESSED_AT = "_PROCESSED_AT"
METADATA_COLUMNS = (SOURCE_URI, FILE_NAME, EXECUTION_PARTITION, RAW_DATA_HASH, INGESTED_AT, PROCESSED_AT)

_PARTITION_PATTERN = rf"{EXECUTION_PARTITION}=([^/]+)"


def dataset_landing_path(volume_path: str, dataset_name: str) -> str:
    return f"{volume_path.rstrip('/')}/raw/{dataset_name}"


def manifest_path(volume_path: str) -> str:
    return f"{volume_path.rstrip('/')}/_manifest/manifest.json"


def read_text_stream(spark: SparkSession, path: str, whole_text: bool = False) -> DataFrame:
    """Incrementally read landed files with Auto Loader as text, adding the source path."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholeText", str(whole_text).lower())
        .option("cloudFiles.partitionColumns", "")
        .load(path)
    )
    return stream.select(F.col("value").alias(RAW_LINE), F.col("_metadata.file_path").alias("_file_path"))


def read_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.table(table_name)


def read_json_file(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("multiLine", True).json(path)


def write_to_uc_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """Write a table outside the pipeline. Pipeline tables are written by the pipeline itself."""
    df.write.mode(mode).option("overwriteSchema", "true").saveAsTable(table_name)


def drop_blank_and_header_lines(df: DataFrame, first_column: str) -> DataFrame:
    """Remove blank lines and header rows. Headers can be padded or capitalised differently."""
    first_field = F.lower(F.trim(F.split(F.col(RAW_LINE), "\t", -1).getItem(0)))
    return df.filter(F.trim(F.col(RAW_LINE)) != "").filter(first_field != first_column.lower())


def explode_json_records(df: DataFrame, record_field: str = "data") -> DataFrame:
    """Turn a whole JSON document into one raw_line per record, leaving record contents as text."""
    parsed = F.from_json(F.col(RAW_LINE), f"{record_field} ARRAY<STRING>")
    return df.withColumn(RAW_LINE, F.explode(parsed.getField(record_field)))


def add_raw_metadata(df: DataFrame, source_url: str, url_is_directory: bool) -> DataFrame:
    """Stamp raw rows. The file name and execution partition come from the immutable landing path."""
    file_name = F.element_at(F.split(F.col("_file_path"), "/"), -1)
    source_uri = F.concat(F.lit(source_url), file_name) if url_is_directory else F.lit(source_url)
    return (
        df.withColumn(FILE_NAME, file_name)
        .withColumn(SOURCE_URI, source_uri)
        .withColumn(EXECUTION_PARTITION, F.regexp_extract(F.col("_file_path"), _PARTITION_PATTERN, 1))
        .withColumn(RAW_DATA_HASH, F.sha2(F.col(RAW_LINE), 256))
        .withColumn(INGESTED_AT, F.current_timestamp())
        .withColumn(PROCESSED_AT, F.current_timestamp())
        .drop("_file_path")
    )


def active_snapshot(raw: DataFrame, manifest: DataFrame) -> DataFrame:
    """Keep only rows from the file versions the manifest marks active.

    This is how a changed file replaces its previous version and a removed file disappears, even
    though the raw table is append-only.
    """
    active = manifest.filter(F.col("status") == "active").select(
        F.col("file_name").alias(FILE_NAME), F.col("execution_partition").alias(EXECUTION_PARTITION)
    )
    return raw.join(active, on=[FILE_NAME, EXECUTION_PARTITION], how="left_semi")


def split_delimited(df: DataFrame, columns: Iterable[str], delimiter: str = "\t") -> DataFrame:
    """Add one trimmed string column per position in the delimited raw line."""
    fields = F.split(F.col(RAW_LINE), delimiter, -1)
    for position, name in enumerate(columns):
        df = df.withColumn(name, F.trim(fields.getItem(position)))
    return df


def empty_strings_to_null(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    for name in columns:
        df = df.withColumn(name, F.nullif(F.col(name), F.lit("")))
    return df


def try_cast_columns(df: DataFrame, column_types: dict[str, str]) -> DataFrame:
    """Cast with try_cast so a bad value becomes NULL for expectations to catch, not a crash."""
    for name, data_type in column_types.items():
        if data_type != "STRING":
            df = df.withColumn(name, F.expr(f"try_cast(`{name}` AS {data_type})"))
    return df


def deduplicate(df: DataFrame, keys: Iterable[str]) -> DataFrame:
    """Keep one row per key, preferring the latest ingestion. The hash makes the choice repeatable."""
    window = Window.partitionBy(*keys).orderBy(F.col(EXECUTION_PARTITION).desc(), F.col(RAW_DATA_HASH).desc())
    return df.withColumn("_row_rank", F.row_number().over(window)).filter("_row_rank = 1").drop("_row_rank")


def select_with_metadata(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Select business columns plus metadata, restamping _PROCESSED_AT for the new layer."""
    return df.withColumn(PROCESSED_AT, F.current_timestamp()).select(*columns, *METADATA_COLUMNS)


def add_final_metadata(df: DataFrame, inputs: Iterable[DataFrame]) -> DataFrame:
    """Stamp a final table: its own row hash, the newest input times, and every contributing source."""
    lineage = None
    for source in inputs:
        selected = source.select(SOURCE_URI, FILE_NAME, EXECUTION_PARTITION, INGESTED_AT)
        lineage = selected if lineage is None else lineage.unionByName(selected)
    lineage = lineage.agg(
        F.array_join(F.sort_array(F.collect_set(SOURCE_URI)), ", ").alias(SOURCE_URI),
        F.array_join(F.sort_array(F.collect_set(FILE_NAME)), ", ").alias(FILE_NAME),
        F.max(EXECUTION_PARTITION).alias(EXECUTION_PARTITION),
        F.max(INGESTED_AT).alias(INGESTED_AT),
    )
    business = df.columns
    return (
        df.crossJoin(F.broadcast(lineage))
        .withColumn(RAW_DATA_HASH, F.sha2(F.to_json(F.struct(*business)), 256))
        .withColumn(PROCESSED_AT, F.current_timestamp())
        .select(*business, *METADATA_COLUMNS)
    )
