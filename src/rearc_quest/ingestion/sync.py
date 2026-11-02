"""Land source files in immutable folders. Files first, manifest last, so a crash is safe to re-run."""

from __future__ import annotations

import hashlib
import logging
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime

from rearc_quest.config.dataset_config_definition import (
    DATASETS,
    HTTP_API,
    HTTP_DIRECTORY,
    POPULATION_FILE_NAME,
    UNMAPPED_DATASET,
    DatasetConfig,
    get_dataset,
    route_file,
)
from rearc_quest.ingestion.file_listing import ListedFile, parse_listing
from rearc_quest.ingestion.manifest import (
    ACTIVE,
    REMOVED,
    load_manifest,
    plan_directory_sync,
    save_manifest,
)
from rearc_quest.ingestion.schema_check import validate_source_schema
from rearc_quest.ingestion.storage import Storage

log = logging.getLogger(__name__)


def new_execution_partition(now: datetime | None = None) -> str:
    """Unique per run, so two runs on one day never overwrite each other's files."""
    return (now or datetime.now(UTC)).strftime("%Y-%m-%dT%H%M%SZ")


def landing_path(dataset_name: str, execution_partition: str, file_name: str) -> str:
    return f"raw/{dataset_name}/_EXECUTION_PARTITION={execution_partition}/{file_name}"


def _land(
    storage: Storage,
    manifest: dict[str, dict],
    *,
    file_name: str,
    dataset_name: str,
    source_url: str,
    body: bytes,
    execution_partition: str,
    modified: str | None,
    size: int,
) -> str:
    """Store a file unless its content is unchanged. Returns 'added', 'changed' or 'unchanged'."""
    sha256 = hashlib.sha256(body).hexdigest()
    previous = manifest.get(file_name)
    if previous is not None and previous["status"] == ACTIVE and previous["sha256"] == sha256:
        previous.update(source_url=source_url, source_modified=modified, size=size)
        return "unchanged"
    storage.write_bytes(landing_path(dataset_name, execution_partition, file_name), body)
    manifest[file_name] = {
        "file_name": file_name,
        "dataset": dataset_name,
        "source_url": source_url,
        "execution_partition": execution_partition,
        "sha256": sha256,
        "source_modified": modified,
        "size": size,
        "status": ACTIVE,
    }
    return "changed" if previous is not None and previous["status"] == ACTIVE else "added"


def sync_directory(
    client, storage: Storage, manifest: dict[str, dict], source_url: str, execution_partition: str
) -> Counter:
    listed: dict[str, ListedFile] = parse_listing(client.get(source_url).decode("utf-8", "replace"))
    plan = plan_directory_sync(listed, manifest, source_url)
    counts = Counter(unchanged=len(plan.unchanged))
    for name in plan.to_download:
        dataset_name = route_file(name, source_url)
        if dataset_name == UNMAPPED_DATASET:
            log.warning("New file %s matches no dataset config; landing it under %s", name, dataset_name)
        body = client.get(source_url + name)
        if dataset_name != UNMAPPED_DATASET:
            validate_source_schema(body, get_dataset(dataset_name))
        outcome = _land(
            storage,
            manifest,
            file_name=name,
            dataset_name=dataset_name,
            source_url=source_url,
            body=body,
            execution_partition=execution_partition,
            modified=listed[name].modified,
            size=listed[name].size,
        )
        counts[outcome] += 1
    for name in plan.removed:
        manifest[name]["status"] = REMOVED
        counts["removed"] += 1
    return counts


def sync_api(
    client,
    storage: Storage,
    manifest: dict[str, dict],
    dataset: DatasetConfig,
    execution_partition: str,
) -> Counter:
    """APIs have no listing, so the response is always fetched and then compared by hash."""
    body = client.get(dataset.source_url)
    validate_source_schema(body, dataset)
    outcome = _land(
        storage,
        manifest,
        file_name=POPULATION_FILE_NAME,
        dataset_name=dataset.name,
        source_url=dataset.source_url,
        body=body,
        execution_partition=execution_partition,
        modified=None,
        size=len(body),
    )
    return Counter({outcome: 1})


def run_ingestion(
    client_factory: Callable[[DatasetConfig], object],
    storage: Storage,
    datasets: tuple[DatasetConfig, ...] = DATASETS,
    execution_partition: str | None = None,
) -> Counter:
    """Sync every configured source and return counts of added, changed, unchanged and removed files."""
    execution_partition = execution_partition or new_execution_partition()
    manifest = load_manifest(storage)
    totals: Counter = Counter()
    seen_directories = set()
    for dataset in datasets:
        if dataset.source_type == HTTP_DIRECTORY and dataset.source_url in seen_directories:
            continue
        client = client_factory(dataset)
        if dataset.source_type == HTTP_DIRECTORY:
            seen_directories.add(dataset.source_url)
            totals += sync_directory(client, storage, manifest, dataset.source_url, execution_partition)
        elif dataset.source_type == HTTP_API:
            totals += sync_api(client, storage, manifest, dataset, execution_partition)
        else:
            raise ValueError(f"Unknown source type: {dataset.source_type}")
        save_manifest(storage, manifest)
    log.info("Ingestion %s finished: %s", execution_partition, dict(totals))
    return totals
