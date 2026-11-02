"""Entry point for ingestion: a wheel task on Databricks, or a command run locally."""

from __future__ import annotations

import argparse
import logging

from rearc_quest.ingestion.storage import FilesApiStorage, LocalStorage
from rearc_quest.ingestion.sync import run_ingestion
from rearc_quest.utils.http_utils import HttpClient, build_user_agent


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Sync BLS files and population JSON into a Volume.")
    parser.add_argument("--volume-path", required=True, help="/Volumes/... or a local folder")
    parser.add_argument(
        "--use-files-api",
        action="store_true",
        help="Upload through the Databricks Files API, for running outside Databricks",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    storage = FilesApiStorage(args.volume_path) if args.use_files_api else LocalStorage(args.volume_path)
    run_ingestion(lambda dataset: HttpClient(build_user_agent(dataset)), storage)


if __name__ == "__main__":
    main()
