"""Registers every pipeline table from config. All logic lives in the rearc_quest wheel.

raw_*       append-only streaming tables, fed by Auto Loader from immutable landing folders
transform_* materialized views of the active file versions, typed and validated
final_*     materialized views answering the three questions
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from rearc_quest.config.dataset_config_definition import DATASETS, FINAL_TABLES, HTTP_DIRECTORY
from rearc_quest.transforms.registry import TRANSFORM_METHODS, build_final
from rearc_quest.utils import spark_utils as su

spark = SparkSession.getActiveSession()
VOLUME_PATH = spark.conf.get("rearc.volume_path")


def _apply_expectations(function, *, fail=None, drop=None, warn=None):
    if fail:
        function = dp.expect_all_or_fail(fail)(function)
    if drop:
        function = dp.expect_all_or_drop(drop)(function)
    if warn:
        function = dp.expect_all(warn)(function)
    return function


def _register_raw(dataset):
    def build():
        path = su.dataset_landing_path(VOLUME_PATH, dataset.name)
        if dataset.file_format == "json":
            stream = su.explode_json_records(su.read_text_stream(spark, path, whole_text=True))
        else:
            stream = su.drop_blank_and_header_lines(su.read_text_stream(spark, path), next(iter(dataset.columns)))
        return su.add_raw_metadata(stream, dataset.source_url, dataset.source_type == HTTP_DIRECTORY)

    build.__name__ = dataset.raw_table_name
    dp.table(name=dataset.raw_table_name, comment=f"Raw rows for {dataset.name}, as landed")(
        _apply_expectations(build, fail=dataset.expectations_fail)
    )


def _register_transform(dataset):
    def build():
        manifest = su.read_json_file(spark, su.manifest_path(VOLUME_PATH))
        raw = su.active_snapshot(su.read_table(spark, dataset.raw_table_name), manifest)
        deps = {name: su.read_table(spark, name) for name in dataset.depends_on}
        return TRANSFORM_METHODS[dataset.transform_method](raw, deps, dataset)

    build.__name__ = dataset.transform_table_name
    dp.materialized_view(name=dataset.transform_table_name, comment=f"Clean {dataset.name}")(
        _apply_expectations(build, drop=dataset.expectations_drop, warn=dataset.expectations_warn)
    )


def _register_final(config):
    def build():
        deps = {name: su.read_table(spark, name) for name in config.depends_on}
        answer = build_final(spark, config, deps, config.primary_language)
        return su.add_final_metadata(answer, deps.values())

    build.__name__ = config.name
    dp.materialized_view(name=config.name, comment=f"Answer: {config.transform_method}")(
        _apply_expectations(build, fail=config.expectations_fail)
    )


for _dataset in DATASETS:
    if _dataset.is_modeled:
        _register_raw(_dataset)
        _register_transform(_dataset)

for _config in FINAL_TABLES:
    _register_final(_config)
