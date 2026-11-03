"""Post-pipeline job: run the quality checks and save the results."""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pyspark.sql import SparkSession

from rearc_quest.quality.checks import run_all_checks
from rearc_quest.utils import spark_utils as su

QUALITY_RESULTS_TABLE = "quality_results"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger(__name__)
    started = time.monotonic()
    log.info("Quality check job starting: catalog=%s schema=%s", args.catalog, args.schema)

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    results = run_all_checks(spark)
    rows = [(r.table, r.check, r.failing_rows, r.passed) for r in results]
    summary = spark.createDataFrame(rows, "table STRING, check STRING, failing_rows BIGINT, passed BOOLEAN")
    su.write_to_uc_table(summary, QUALITY_RESULTS_TABLE)
    log.info("Wrote %d check results to %s.%s.%s", len(rows), args.catalog, args.schema, QUALITY_RESULTS_TABLE)

    failed = [r for r in results if not r.passed]
    for result in failed:
        print(f"FAILED {result.table}.{result.check}: {result.failing_rows} rows")
    print(f"{len(results) - len(failed)} of {len(results)} checks passed")
    log.info(
        "Run summary: status=%s duration=%.1fs checks=%d passed=%d failed=%d",
        "FAILED" if failed else "SUCCEEDED",
        time.monotonic() - started,
        len(results),
        len(results) - len(failed),
        len(failed),
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
