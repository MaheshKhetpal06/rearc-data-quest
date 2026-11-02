"""Runs the Spark SQL answers on DuckDB, which needs no JVM. The SQL used is common to both engines."""

from importlib import resources

import pytest

duckdb = pytest.importorskip("duckdb")


def load(name: str) -> str:
    return resources.files("rearc_quest.sql").joinpath(f"{name}.sql").read_text()


@pytest.fixture
def db():
    con = duckdb.connect()
    con.execute("CREATE TABLE transform_bls_data (series_id VARCHAR, year INT, period VARCHAR, value DECIMAL(12,3))")
    con.execute("CREATE TABLE transform_bls_series (series_id VARCHAR, series_label VARCHAR)")
    con.execute("CREATE TABLE transform_population (year INT, population BIGINT)")
    yield con
    con.close()


def add_data(con, rows):
    con.executemany("INSERT INTO transform_bls_data VALUES (?, ?, ?, ?)", rows)


def add_series(con, *ids):
    con.executemany("INSERT INTO transform_bls_series VALUES (?, ?)", [(i, f"label {i}") for i in ids])


def best_year(con):
    return {row[0]: row for row in con.execute(load("series_best_year")).fetchall()}


def test_best_year_sums_quarters(db):
    add_series(db, "A")
    add_data(db, [("A", 2000, "Q01", 1), ("A", 2000, "Q02", 1), ("A", 2001, "Q01", 2), ("A", 2001, "Q02", 3)])
    row = best_year(db)["A"]
    assert (row[2], float(row[3]), row[4], row[5]) == (2001, 5.0, 2, True)


def test_best_year_ignores_annual_average(db):
    add_series(db, "A")
    add_data(db, [("A", 2000, "Q01", 5), ("A", 2001, "Q01", 4), ("A", 2001, "Q05", 100)])
    assert best_year(db)["A"][2] == 2000


def test_best_year_tie_goes_to_latest_year(db):
    add_series(db, "A")
    add_data(db, [("A", 2000, "Q01", 5), ("A", 2001, "Q01", 5)])
    assert best_year(db)["A"][2] == 2001


def test_best_year_handles_all_negative_values(db):
    add_series(db, "A")
    add_data(db, [("A", 2000, "Q01", -5), ("A", 2001, "Q01", -1)])
    assert best_year(db)["A"][2] == 2001


def test_series_with_only_annual_data_is_kept_with_null_best_year(db):
    add_series(db, "A")
    add_data(db, [("A", 2000, "Q05", 5)])
    row = best_year(db)["A"]
    assert row[2] is None
    assert row[5] is False


def test_population_stats_use_only_2013_to_2018_and_sample_stddev(db):
    rows = [(y, v) for y, v in zip(range(2012, 2020), (999, 10, 20, 30, 40, 50, 60, 999), strict=True)]
    db.executemany("INSERT INTO transform_population VALUES (?, ?)", rows)
    result = db.execute(load("population_stats")).fetchone()
    assert result[:3] == (2013, 2018, 6)
    assert result[3] == 35.0
    assert result[4] == pytest.approx(18.71, abs=0.01)


def test_report_filters_series_and_period_and_keeps_years_without_population(db):
    add_data(
        db,
        [
            ("PRS30006032", 2019, "Q01", 1),
            ("PRS30006032", 2020, "Q01", 2),
            ("PRS30006032", 2019, "Q02", 3),
            ("OTHER", 2019, "Q01", 4),
        ],
    )
    db.execute("INSERT INTO transform_population VALUES (2019, 100)")
    rows = sorted(db.execute(load("series_population_report")).fetchall(), key=lambda r: r[1])
    assert [(r[0], r[1], r[2], float(r[3]), r[4]) for r in rows] == [
        ("PRS30006032", 2019, "Q01", 1.0, 100),
        ("PRS30006032", 2020, "Q01", 2.0, None),
    ]
