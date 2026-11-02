import json

import pytest

from rearc_quest.config.dataset_config_definition import get_dataset
from rearc_quest.ingestion.schema_check import SourceSchemaError, validate_source_schema

DATA_HEADER = b"series_id        \tyear\tperiod\t       value\tfootnote_codes\r\nPRS1\t2000\tQ01\t1\t\r\n"


def test_accepts_padded_header_with_crlf():
    validate_source_schema(DATA_HEADER, get_dataset("bls_data"))


def test_accepts_capitalised_header():
    validate_source_schema(b"Seasonal_code\tSeasonal_text\r\nS\tx\r\n", get_dataset("bls_seasonal"))


def test_rejects_reordered_columns_with_the_same_count():
    body = b"series_id\tperiod\tyear\tvalue\tfootnote_codes\n"
    with pytest.raises(SourceSchemaError, match="bls_data"):
        validate_source_schema(body, get_dataset("bls_data"))


def test_rejects_added_column():
    body = b"series_id\tyear\tperiod\tvalue\tfootnote_codes\textra\n"
    with pytest.raises(SourceSchemaError):
        validate_source_schema(body, get_dataset("bls_data"))


def test_rejects_empty_file():
    with pytest.raises(SourceSchemaError):
        validate_source_schema(b"", get_dataset("bls_data"))


def test_population_checks_the_columns_field():
    good = {"columns": ["Nation ID", "Nation", "Year", "Population"], "data": []}
    validate_source_schema(json.dumps(good).encode(), get_dataset("population"))
    with pytest.raises(SourceSchemaError):
        validate_source_schema(b'{"columns": ["Year"], "data": []}', get_dataset("population"))


def test_population_rejects_invalid_json():
    with pytest.raises(SourceSchemaError, match="not valid JSON"):
        validate_source_schema(b"<html>error</html>", get_dataset("population"))


def test_unmodeled_files_are_not_checked():
    validate_source_schema(b"free text", get_dataset("bls_docs"))
