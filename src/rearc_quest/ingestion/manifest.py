"""The manifest records every file's current version and decides what each run has to do."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from rearc_quest.ingestion.file_listing import ListedFile
from rearc_quest.ingestion.storage import Storage

MANIFEST_PATH = "_manifest/manifest.json"
ACTIVE = "active"
REMOVED = "removed"


@dataclass
class SyncPlan:
    to_download: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)


def load_manifest(storage: Storage) -> dict[str, dict]:
    body = storage.read_bytes(MANIFEST_PATH)
    if body is None:
        return {}
    return {entry["file_name"]: entry for entry in json.loads(body)}


def save_manifest(storage: Storage, manifest: dict[str, dict]) -> None:
    """Saved as a JSON array so Spark can read it with multiLine."""
    entries = [manifest[name] for name in sorted(manifest)]
    storage.write_bytes(MANIFEST_PATH, json.dumps(entries, indent=2).encode("utf-8"))


def plan_directory_sync(listed: dict[str, ListedFile], manifest: dict[str, dict], source_url: str) -> SyncPlan:
    """Compare a listing with the manifest. Listing metadata lets unchanged files skip the download."""
    plan = SyncPlan()
    for name, item in sorted(listed.items()):
        entry = manifest.get(name)
        if (
            entry is not None
            and entry["status"] == ACTIVE
            and entry["source_modified"] == item.modified
            and entry["size"] == item.size
        ):
            plan.unchanged.append(name)
        else:
            plan.to_download.append(name)
    plan.removed = sorted(
        name
        for name, entry in manifest.items()
        if entry["source_url"] == source_url and entry["status"] == ACTIVE and name not in listed
    )
    return plan
