import re

from rearc_quest.config.dataset_config_definition import (
    BLS_URL,
    DATASETS,
    FINAL_TABLES,
    UNMAPPED_DATASET,
    get_dataset,
    get_final_table,
    route_file,
)

SOURCE_FILES = {
    "pr.class": "bls_class",
    "pr.contacts": "bls_docs",
    "pr.data.0.Current": "bls_current",
    "pr.data.1.AllData": "bls_data",
    "pr.duration": "bls_duration",
    "pr.footnote": "bls_footnote",
    "pr.measure": "bls_measure",
    "pr.period": "bls_period",
    "pr.seasonal": "bls_seasonal",
    "pr.sector": "bls_sector",
    "pr.series": "bls_series",
    "pr.txt": "bls_docs",
}


def test_route_file_covers_every_current_source_file():
    assert {name: route_file(name, BLS_URL) for name in SOURCE_FILES} == SOURCE_FILES


def test_route_file_sends_unknown_files_to_unmapped():
    assert route_file("pr.brand_new", BLS_URL) == UNMAPPED_DATASET


def test_current_is_landed_but_not_modeled_to_avoid_duplicates():
    assert get_dataset("bls_current").is_modeled is False
    assert get_dataset("bls_data").is_modeled is True


def test_modeled_datasets_have_required_fields():
    for dataset in (d for d in DATASETS if d.is_modeled):
        assert dataset.schema_name
        assert dataset.transform_table_name
        assert dataset.transform_method
        assert dataset.columns
        assert dataset.keys
        assert set(dataset.keys) <= set(dataset.columns)
        assert re.compile(dataset.file_pattern)


def test_dataset_names_are_unique():
    names = [d.name for d in DATASETS]
    assert len(names) == len(set(names))


def test_final_tables_depend_only_on_known_transform_tables():
    known = {d.transform_table_name for d in DATASETS}
    for final in FINAL_TABLES:
        assert set(final.depends_on) <= known


def test_dataset_dependencies_are_known_transform_tables():
    known = {d.transform_table_name for d in DATASETS}
    for dataset in DATASETS:
        assert set(dataset.depends_on) <= known


def test_config_holds_only_a_reference_to_the_contact():
    dataset = get_dataset("bls_data")
    assert dataset.credential.scope == "rearc-quest"
    assert "{contact}" in dataset.user_agent_template
    assert "@" not in dataset.user_agent_template


def test_population_needs_no_credential():
    assert get_dataset("population").credential is None


def test_get_final_table_finds_by_name():
    assert get_final_table("final_series_best_year").transform_method == "series_best_year"
