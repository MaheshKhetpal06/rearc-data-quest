"""Check a source file's own column names before it lands, so a changed layout never reaches the tables."""

from __future__ import annotations

import json

from rearc_quest.config.dataset_config_definition import DatasetConfig


class SourceSchemaError(RuntimeError):
    pass


def _actual_columns(body: bytes, dataset: DatasetConfig) -> list[str]:
    text = body.decode("utf-8", "replace")
    if dataset.file_format == "json":
        return list(json.loads(text).get("columns", []))
    header = text.splitlines()[0] if text else ""
    return [name.strip().lower() for name in header.split("\t")]


def validate_source_schema(body: bytes, dataset: DatasetConfig) -> None:
    """Compare names and order with config. Modeled datasets only, as other files have no table."""
    if not dataset.is_modeled:
        return
    expected = list(dataset.source_columns or dataset.columns)
    try:
        actual = _actual_columns(body, dataset)
    except json.JSONDecodeError as exc:
        raise SourceSchemaError(f"{dataset.name}: response is not valid JSON") from exc
    if actual != expected:
        raise SourceSchemaError(f"{dataset.name}: expected columns {expected}, found {actual}")
