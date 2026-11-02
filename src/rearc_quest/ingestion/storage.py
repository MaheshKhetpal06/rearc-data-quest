"""Where landed files go. Volume paths act as local paths on Databricks; off-platform we use the Files API."""

from __future__ import annotations

import io
import os
import pathlib
import uuid
from typing import Protocol


class Storage(Protocol):
    def read_bytes(self, relative_path: str) -> bytes | None: ...

    def write_bytes(self, relative_path: str, body: bytes) -> None: ...


class LocalStorage:
    """A filesystem path: a local folder, or /Volumes/... on Databricks."""

    def __init__(self, root: str):
        self.root = pathlib.Path(root)

    def read_bytes(self, relative_path: str) -> bytes | None:
        path = self.root / relative_path
        return path.read_bytes() if path.exists() else None

    def write_bytes(self, relative_path: str, body: bytes) -> None:
        """Write to a temporary file and rename, so readers never see a partial file."""
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
        temporary.write_bytes(body)
        os.replace(temporary, path)


class FilesApiStorage:
    """A Unity Catalog Volume reached from outside Databricks. Each upload is a single API call."""

    def __init__(self, volume_path: str, client=None):
        if client is None:
            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient()
        self.client = client
        self.root = volume_path.rstrip("/")

    def read_bytes(self, relative_path: str) -> bytes | None:
        from databricks.sdk.errors import NotFound

        try:
            return self.client.files.download(f"{self.root}/{relative_path}").contents.read()
        except NotFound:
            return None

    def write_bytes(self, relative_path: str, body: bytes) -> None:
        self.client.files.upload(f"{self.root}/{relative_path}", io.BytesIO(body), overwrite=True)
