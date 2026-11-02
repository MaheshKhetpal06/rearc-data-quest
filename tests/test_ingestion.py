import json

import pytest

from rearc_quest.config.dataset_config_definition import BLS_URL, POPULATION_URL
from rearc_quest.ingestion.file_listing import ListedFile, ListingError, parse_listing
from rearc_quest.ingestion.manifest import load_manifest, plan_directory_sync, save_manifest
from rearc_quest.ingestion.schema_check import SourceSchemaError
from rearc_quest.ingestion.storage import LocalStorage
from rearc_quest.ingestion.sync import landing_path, new_execution_partition, run_ingestion

POPULATION = b'{"columns": ["Nation ID", "Nation", "Year", "Population"], "data": []}'
ROW = '{modified}  {size} <A HREF="/pub/time.series/pr/{name}">{name}</A><br>'
PARENT = '<A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>'


def listing(files: dict[str, tuple[str, bytes]]) -> bytes:
    rows = [ROW.format(modified=m, size=len(b), name=n) for n, (m, b) in files.items()]
    return ("<pre>" + PARENT + "".join(rows) + "</pre>").encode()


def responses(files: dict[str, tuple[str, bytes]], population: bytes = POPULATION) -> dict[str, bytes]:
    result = {BLS_URL: listing(files), POPULATION_URL: population}
    result.update({BLS_URL + name: body for name, (_, body) in files.items()})
    return result


SERIES = (
    b"series_id\tsector_code\tclass_code\tmeasure_code\tduration_code\tseasonal\tbase_year\t"
    b"footnote_codes\tbegin_year\tbegin_period\tend_year\tend_period\n"
)
DATA = b"series_id\tyear\tperiod\tvalue\tfootnote_codes\n"

FILES = {
    "pr.series": ("9/3/2026  8:30 AM", SERIES),
    "pr.data.1.AllData": ("9/3/2026  8:30 AM", DATA),
}


def run(fake_client, storage, files, partition, population=POPULATION):
    client = fake_client(responses(files, population))
    return run_ingestion(lambda _: client, storage, execution_partition=partition), client


def test_parse_listing_reads_real_format_and_skips_parent_link():
    html = (
        '<pre><A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>  '
        '9/3/2026  8:30 AM          102 <A HREF="/pub/time.series/pr/pr.class">pr.class</A><br>'
        '11/18/2011  4:05 PM           79 <A HREF="/pub/time.series/pr/pr.seasonal">pr.seasonal</A><br></pre>'
    )
    assert parse_listing(html) == {
        "pr.class": ListedFile("pr.class", "9/3/2026 8:30 AM", 102),
        "pr.seasonal": ListedFile("pr.seasonal", "11/18/2011 4:05 PM", 79),
    }


def test_parse_listing_rejects_page_without_files():
    with pytest.raises(ListingError):
        parse_listing("<html>Service unavailable</html>")


def test_plan_classifies_new_changed_unchanged_and_removed():
    active = {"status": "active", "source_url": BLS_URL, "source_modified": "old", "size": 1}
    manifest = {
        "same": {**active, "file_name": "same"},
        "edited": {**active, "file_name": "edited"},
        "gone": {**active, "file_name": "gone"},
        "elsewhere": {**active, "file_name": "elsewhere", "source_url": POPULATION_URL},
    }
    listed = {
        "same": ListedFile("same", "old", 1),
        "edited": ListedFile("edited", "new", 1),
        "fresh": ListedFile("fresh", "new", 1),
    }
    plan = plan_directory_sync(listed, manifest, BLS_URL)
    assert plan.unchanged == ["same"]
    assert plan.to_download == ["edited", "fresh"]
    assert plan.removed == ["gone"]


def test_plan_downloads_again_when_a_removed_file_returns():
    manifest = {
        "back": {
            "file_name": "back",
            "status": "removed",
            "source_url": BLS_URL,
            "source_modified": "old",
            "size": 1,
        }
    }
    plan = plan_directory_sync({"back": ListedFile("back", "old", 1)}, manifest, BLS_URL)
    assert plan.to_download == ["back"]


def test_manifest_round_trip_is_a_json_array(tmp_path):
    storage = LocalStorage(str(tmp_path))
    assert load_manifest(storage) == {}
    save_manifest(storage, {"a": {"file_name": "a", "status": "active"}})
    assert load_manifest(storage)["a"]["status"] == "active"
    assert isinstance(json.loads(storage.read_bytes("_manifest/manifest.json")), list)


def test_local_storage_writes_atomically_and_leaves_no_temp_files(tmp_path):
    storage = LocalStorage(str(tmp_path))
    storage.write_bytes("a/b/file", b"one")
    storage.write_bytes("a/b/file", b"two")
    assert storage.read_bytes("a/b/file") == b"two"
    assert [p.name for p in (tmp_path / "a" / "b").iterdir()] == ["file"]
    assert storage.read_bytes("missing") is None


def test_execution_partition_has_no_characters_unsafe_for_paths():
    assert ":" not in new_execution_partition()


def test_first_run_lands_all_files_and_population(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    totals, _ = run(fake_client, storage, FILES, "P1")
    assert totals["added"] == 3
    assert storage.read_bytes(landing_path("bls_series", "P1", "pr.series")) == SERIES
    assert storage.read_bytes(landing_path("population", "P1", "population.json")) == POPULATION


def test_second_run_downloads_nothing_from_bls(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    totals, client = run(fake_client, storage, FILES, "P2")
    assert totals["added"] == totals["changed"] == totals["removed"] == 0
    assert totals["unchanged"] == 3
    assert client.requests == [BLS_URL, POPULATION_URL]
    assert not (tmp_path / "raw" / "bls_series" / "_EXECUTION_PARTITION=P2").exists()


def test_changed_file_lands_in_new_folder_and_keeps_the_old_version(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    changed = {**FILES, "pr.series": ("9/4/2026  8:30 AM", SERIES + b"row\n")}
    totals, _ = run(fake_client, storage, changed, "P2")
    assert totals["changed"] == 1
    assert storage.read_bytes(landing_path("bls_series", "P1", "pr.series")) == SERIES
    assert storage.read_bytes(landing_path("bls_series", "P2", "pr.series")) == SERIES + b"row\n"
    assert load_manifest(storage)["pr.series"]["execution_partition"] == "P2"


def test_new_timestamp_with_same_content_is_not_reprocessed(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    touched = {**FILES, "pr.series": ("9/4/2026  8:30 AM", SERIES)}
    totals, _ = run(fake_client, storage, touched, "P2")
    assert totals["changed"] == 0
    entry = load_manifest(storage)["pr.series"]
    assert entry["execution_partition"] == "P1"
    assert entry["source_modified"] == "9/4/2026 8:30 AM"


def test_removed_file_is_marked_and_its_data_is_retained(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    remaining = {"pr.series": FILES["pr.series"]}
    totals, _ = run(fake_client, storage, remaining, "P2")
    assert totals["removed"] == 1
    assert load_manifest(storage)["pr.data.1.AllData"]["status"] == "removed"
    assert storage.read_bytes(landing_path("bls_data", "P1", "pr.data.1.AllData")) == DATA


def test_added_file_with_no_config_still_lands(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    extended = {**FILES, "pr.new": ("9/4/2026  8:30 AM", b"new")}
    totals, _ = run(fake_client, storage, extended, "P2")
    assert totals["added"] == 1
    assert storage.read_bytes(landing_path("unmapped", "P2", "pr.new")) == b"new"


def test_bad_listing_never_marks_files_removed(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    client = fake_client({BLS_URL: b"<html>maintenance</html>", POPULATION_URL: b"{}"})
    with pytest.raises(ListingError):
        run_ingestion(lambda _: client, storage, execution_partition="P2")
    assert all(entry["status"] == "active" for entry in load_manifest(storage).values())


def test_population_change_is_detected_by_hash(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    totals, _ = run(fake_client, storage, FILES, "P2", population=POPULATION.replace(b"[]", b"[1]"))
    assert totals["changed"] == 1
    assert load_manifest(storage)["population.json"]["execution_partition"] == "P2"


def test_changed_layout_stops_the_run_before_the_file_or_manifest_is_saved(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    reordered = {**FILES, "pr.series": ("9/4/2026  8:30 AM", b"sector_code\tseries_id\n")}
    with pytest.raises(SourceSchemaError):
        run(fake_client, storage, reordered, "P2")
    assert storage.read_bytes(landing_path("bls_series", "P2", "pr.series")) is None
    assert load_manifest(storage)["pr.series"]["execution_partition"] == "P1"


def test_manifest_source_url_follows_config_when_the_content_is_unchanged(tmp_path, fake_client):
    storage = LocalStorage(str(tmp_path))
    run(fake_client, storage, FILES, "P1")
    manifest = load_manifest(storage)
    manifest["population.json"]["source_url"] = "https://old.example.com"
    save_manifest(storage, manifest)
    run(fake_client, storage, FILES, "P2")
    assert load_manifest(storage)["population.json"]["source_url"] == POPULATION_URL
