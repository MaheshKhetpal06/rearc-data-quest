"""The single source of configuration for every dataset and final table.

Files are matched by pattern rather than listed by name, so a file BLS adds later still lands in the
Volume. Only the secret's location is stored here, never its value.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

DEFAULT_SCHEMA = "rearc_quest"
USER_AGENT_PLAIN = "rearc-data-quest/1.0"
USER_AGENT_WITH_CONTACT = "rearc-data-quest/1.0 (contact: {contact})"

BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_URL = (
    "https://api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
POPULATION_FILE_NAME = "population.json"
UNMAPPED_DATASET = "unmapped"

HTTP_DIRECTORY = "http_directory"
HTTP_API = "http_api"


@dataclass(frozen=True)
class SecretRef:
    """Where to find a secret: an environment variable locally, a secret scope on Databricks."""

    scope: str
    key: str
    env_var: str


BLS_CONTACT = SecretRef(scope="rearc-quest", key="bls-contact-email", env_var="BLS_CONTACT_EMAIL")


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    source_type: str
    source_url: str
    file_pattern: str
    schema_name: str = DEFAULT_SCHEMA
    file_format: str = "tsv"
    credential: SecretRef | None = None
    user_agent_template: str = USER_AGENT_PLAIN
    raw_table_name: str | None = None
    transform_table_name: str | None = None
    final_table_name: str | None = None
    transform_method: str | None = None
    columns: dict[str, str] = field(default_factory=dict)
    # Column names as the source spells them, where they differ from the table columns.
    source_columns: tuple[str, ...] = ()
    keys: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    expectations_fail: dict[str, str] = field(default_factory=dict)
    expectations_drop: dict[str, str] = field(default_factory=dict)
    expectations_warn: dict[str, str] = field(default_factory=dict)

    @property
    def is_modeled(self) -> bool:
        return self.raw_table_name is not None


@dataclass(frozen=True)
class FinalTableConfig:
    name: str
    transform_method: str
    depends_on: tuple[str, ...]
    keys: tuple[str, ...]
    schema_name: str = DEFAULT_SCHEMA
    primary_language: str = "sql"
    expectations_fail: dict[str, str] = field(default_factory=dict)


def _field_count_check(column_count: int) -> dict[str, str]:
    return {"expected_field_count": f"size(split(raw_line, '\\t', -1)) = {column_count}"}


def _bls_file(name: str, pattern: str, **kwargs) -> DatasetConfig:
    return DatasetConfig(
        name=name,
        source_type=HTTP_DIRECTORY,
        source_url=BLS_URL,
        file_pattern=pattern,
        credential=BLS_CONTACT,
        user_agent_template=USER_AGENT_WITH_CONTACT,
        **kwargs,
    )


def _bls_lookup(short_name: str, columns: dict[str, str]) -> DatasetConfig:
    key = next(iter(columns))
    return _bls_file(
        f"bls_{short_name}",
        rf"^pr\.{short_name}$",
        raw_table_name=f"raw_bls_{short_name}",
        transform_table_name=f"transform_bls_{short_name}",
        transform_method="bls_lookup",
        columns=columns,
        keys=(key,),
        expectations_fail=_field_count_check(len(columns)),
        expectations_drop={"key_not_null": f"{key} IS NOT NULL"},
    )


_DISPLAY = {"display_level": "STRING", "selectable": "STRING", "sort_sequence": "STRING"}
_DATA_COLUMNS = {
    "series_id": "STRING",
    "year": "INT",
    "period": "STRING",
    # DECIMAL keeps sums exact, so best-year ties and SQL/PySpark comparisons are not float noise.
    "value": "DECIMAL(12,3)",
    "footnote_codes": "STRING",
}
_SERIES_COLUMNS = {
    "series_id": "STRING",
    "sector_code": "STRING",
    "class_code": "STRING",
    "measure_code": "STRING",
    "duration_code": "STRING",
    "seasonal": "STRING",
    "base_year": "STRING",
    "footnote_codes": "STRING",
    "begin_year": "INT",
    "begin_period": "STRING",
    "end_year": "INT",
    "end_period": "STRING",
}
_POPULATION_COLUMNS = {"nation_id": "STRING", "nation": "STRING", "year": "INT", "population": "BIGINT"}

DATASETS: tuple[DatasetConfig, ...] = (
    _bls_file(
        "bls_data",
        r"^pr\.data\.\d+\.AllData$",
        raw_table_name="raw_bls_data",
        transform_table_name="transform_bls_data",
        final_table_name="final_series_best_year",
        transform_method="bls_data",
        columns=_DATA_COLUMNS,
        keys=("series_id", "year", "period"),
        expectations_fail=_field_count_check(len(_DATA_COLUMNS)),
        expectations_drop={
            "series_id_not_null": "series_id IS NOT NULL",
            "year_not_null": "year IS NOT NULL",
            "valid_period": "period RLIKE '^Q0[1-5]$'",
            "value_not_null": "value IS NOT NULL",
        },
    ),
    # A subset of AllData (verified by scripts/profile_sources.py). It lands but is not modeled,
    # because loading both would duplicate rows.
    _bls_file("bls_current", r"^pr\.data\.\d+\.Current$"),
    _bls_file(
        "bls_series",
        r"^pr\.series$",
        raw_table_name="raw_bls_series",
        transform_table_name="transform_bls_series",
        transform_method="bls_series",
        columns=_SERIES_COLUMNS,
        keys=("series_id",),
        depends_on=(
            "transform_bls_sector",
            "transform_bls_measure",
            "transform_bls_duration",
            "transform_bls_class",
        ),
        expectations_fail=_field_count_check(len(_SERIES_COLUMNS)),
        expectations_drop={"series_id_not_null": "series_id IS NOT NULL"},
        expectations_warn={
            "sector_label_found": "sector_name IS NOT NULL",
            "measure_label_found": "measure_text IS NOT NULL",
            "duration_label_found": "duration_text IS NOT NULL",
            "class_label_found": "class_text IS NOT NULL",
        },
    ),
    _bls_lookup("class", {"class_code": "STRING", "class_text": "STRING", **_DISPLAY}),
    _bls_lookup("duration", {"duration_code": "STRING", "duration_text": "STRING", **_DISPLAY}),
    _bls_lookup("footnote", {"footnote_code": "STRING", "footnote_text": "STRING"}),
    _bls_lookup("measure", {"measure_code": "STRING", "measure_text": "STRING", **_DISPLAY}),
    _bls_lookup("period", {"period": "STRING", "period_abbr": "STRING", "period_name": "STRING"}),
    _bls_lookup("seasonal", {"seasonal_code": "STRING", "seasonal_text": "STRING"}),
    _bls_lookup("sector", {"sector_code": "STRING", "sector_name": "STRING", **_DISPLAY}),
    # Free-text documentation: landed for completeness, no table.
    _bls_file("bls_docs", r"^pr\.(txt|contacts)$"),
    DatasetConfig(
        name="population",
        source_type=HTTP_API,
        source_url=POPULATION_URL,
        file_pattern=rf"^{re.escape(POPULATION_FILE_NAME)}$",
        file_format="json",
        raw_table_name="raw_population",
        transform_table_name="transform_population",
        final_table_name="final_population_stats",
        transform_method="population",
        columns=_POPULATION_COLUMNS,
        source_columns=("Nation ID", "Nation", "Year", "Population"),
        keys=("year",),
        expectations_fail={"raw_line_not_null": "raw_line IS NOT NULL"},
        expectations_drop={
            "year_not_null": "year IS NOT NULL",
            "population_positive": "population > 0",
        },
    ),
)

FINAL_TABLES: tuple[FinalTableConfig, ...] = (
    FinalTableConfig(
        name="final_population_stats",
        transform_method="population_stats",
        depends_on=("transform_population",),
        keys=("min_year", "max_year"),
        expectations_fail={"all_six_years_present": "years_included = 6"},
    ),
    FinalTableConfig(
        name="final_series_best_year",
        transform_method="series_best_year",
        depends_on=("transform_bls_data", "transform_bls_series"),
        keys=("series_id",),
        expectations_fail={
            "series_id_not_null": "series_id IS NOT NULL",
            "label_not_empty": "series_label IS NOT NULL AND series_label <> ''",
        },
    ),
    FinalTableConfig(
        name="final_series_population_report",
        transform_method="series_population_report",
        depends_on=("transform_bls_data", "transform_population"),
        keys=("series_id", "year", "period"),
        expectations_fail={"year_not_null": "year IS NOT NULL"},
    ),
)


def get_dataset(name: str) -> DatasetConfig:
    for dataset in DATASETS:
        if dataset.name == name:
            return dataset
    raise KeyError(f"Unknown dataset: {name}")


def get_final_table(name: str) -> FinalTableConfig:
    for table in FINAL_TABLES:
        if table.name == name:
            return table
    raise KeyError(f"Unknown final table: {name}")


def route_file(file_name: str, source_url: str, datasets: tuple[DatasetConfig, ...] = DATASETS) -> str:
    """Return the dataset owning a file from this source, or UNMAPPED_DATASET if none matches."""
    for dataset in datasets:
        if dataset.source_url == source_url and re.fullmatch(dataset.file_pattern, file_name):
            return dataset.name
    return UNMAPPED_DATASET
