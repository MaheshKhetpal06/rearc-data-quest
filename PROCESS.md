# Process

> **Status:** deployed with an Asset Bundle to Databricks Free Edition and run end to end on 1 Nov 2026. Still to
> do before submitting: the Retrospective, the AI usage details and the screenshots.

## The data
Profiled once with `scripts/profile_sources.py` before any modeling (figures from 1 Nov 2026).

| Source | Fetch | Size | Notes |
|---|---|---|---|
| BLS `pr` folder | Batch file download | 12 files, 4.89 MB | `pr.data.1.AllData` has 77,126 rows across 282 series |
| Population API | JSON API, no key | 1.4 KB, 11 rows | 2013-2024, with 2020 missing |

Findings that shaped the design:
- **`pr.data.0.Current` is a subset of `AllData`.** All 38,469 of its rows appear in `AllData` with identical
  values. `pr.txt` calls it year-to-date, but it goes back to 1995. Modeling both would duplicate rows, so it lands
  but is not modeled.
- **`Q05` is an annual average** (`pr.period`). Summing it with the quarters would change the best year for
  11 series.
- **45 series have only `Q05` data**, so they have no quarters to sum.
- **Population is a survey estimate, and which one matters.** I used the ACS 1-year estimate because the question
  needs one figure per year. The 5-year estimate pools overlapping years and gives a 2013-2018 mean about 4.6 million
  lower (317,437,383 vs 322,069,808). The 1-year data has no 2020 row, which stays NULL rather than being filled from
  a different measure.
- **API address:** I use `api.datausa.io`, the address in DataUSA's documentation. It returned the same data as the
  address in the quest brief when I compared them on 1 Nov 2026.
- **Formatting:** `series_id` and `value` are padded, the header names are padded as well, lines end in CRLF,
  `pr.seasonal` has a trailing blank line, and its header capitalisation differs from the other files.
- **No duplicate keys, non-numeric values or missing lookup codes** exist today. Checks stay in place in case
  that changes.

## Architecture
**Flow:** source → Volume landing folders → `raw_*` → `transform_*` → `final_*`. These layers correspond to
bronze, silver and gold.

| Layer | Type | Contents |
|---|---|---|
| Volume | Files | Every source file, unchanged, in immutable `raw/<dataset>/_EXECUTION_PARTITION=<run>/` folders |
| `raw_*` | Streaming tables (Auto Loader) | One row per source line or JSON record as text, append-only |
| `transform_*` | Materialized views | The active file version, typed, deduplicated, validated, with labels |
| `final_*` | Materialized views | The three answers |

**Config-driven.** `dataset_config_definition.py` is the one place that defines datasets, tables, transform
methods, sources, expectations and the reference to the BLS contact secret. `src/pipeline/definitions.py` loops
over it, so a new lookup file needs a config entry rather than new pipeline code.

**Raw keeps text.** Keeping the original line means a parsing bug can be fixed without fetching the data again,
and it gives `_RAW_DATA_HASH` a clear meaning. Files are read by position, which sidesteps the padded headers.

**Metadata columns in all three layers:**

| Column | Meaning |
|---|---|
| `_SOURCE_URI` | Where the row came from: the BLS file URL or the API URL. Final tables list every contributing source |
| `_FILE_NAME` | The landed file holding the row. Final tables list every contributing file |
| `_EXECUTION_PARTITION` | The ingestion run that landed the data (newest input, in final tables) |
| `_RAW_DATA_HASH` | SHA-256 of the raw line, carried through transform. Final tables have no single raw line, so it hashes the output row |
| `_INGESTED_AT` | When the row entered raw (newest input, in final tables) |
| `_PROCESSED_AT` | When that layer wrote the row |

`_EXECUTION_PARTITION` is a regular column, not a physical partition. At about 77,000 rows, partitioning would
only create small files.

**Re-running ingestion safely:**
1. The BLS listing is parsed, so no filename is hardcoded. Files matching no config land under `unmapped/` with a
   warning.
2. Unchanged size and modified time means no download. Otherwise the file is downloaded and hashed, so a new
   timestamp on identical content lands nothing.
3. New content goes in a new folder and is never overwritten, so Auto Loader only sees each version once.
4. The manifest is written last, so a crash leaves unreferenced files and the next run simply repeats the work.
5. A file gone from the source is marked `removed` in the manifest and its landed copy is kept. A listing with no
   files raises an error rather than looking like everything was deleted.

Transform tables read only the versions the manifest marks active. That's how an append-only raw table can still
reflect changed and removed files.

**SQL or PySpark.** SQL is primary for all three answers because the logic reads naturally as set operations and
is easy for an analyst to review. The PySpark versions are in `transforms/final_tables.py`, and
`jobs/validate_equivalence.py` compares both with `exceptAll` in both directions. Everything around the answers is
PySpark, because looping over config is much easier in Python.

**Decisions on the answers:**
- **Annual-only series:** kept with a NULL best year and `has_quarterly_data = false`, rather than dropped or
  answered with annual averages.
- **Partial years:** included, as the question says to sum the quarters. This decides the best year for 1 series.
  `quarters_in_best_year` makes it visible.
- **Ties:** the latest year wins. 2 series are affected.
- **Standard deviation:** sample, 4,158,441.04, as six years are a sample of the population series. The population
  version would be 3,796,119.94.
- **Report join:** a left join, so years with no population stay. 11 of the 39 rows have a population.
- **`value` is `DECIMAL(12,3)`** so sums are exact and ties are real.
- **A caveat on meaning:** two-thirds of series are percent changes, and summing those isn't economically
  meaningful. The label names the duration so readers can see that.

**Data quality.** Ingestion rejects a file whose column names or order differ from config. Expectations fail on an unexpected field count at raw, drop rows with null keys or bad types at
transform, warn on missing lookup labels, and fail on an invalid final table. `try_cast` turns bad values into
NULLs so expectations can count them instead of the run crashing. `quality/checks.py` adds simple checks after the
pipeline. Results are saved to `quality_results`.

**Access to BLS.** BLS rejects requests without contact details, so the client sends
`rearc-data-quest/1.0 (contact: ...)` as its policy asks, and pauses between requests. The contact stays out of the
repo, in an environment variable or a Databricks secret.

## Free Edition constraints
- **Serverless only**, so compute size isn't a working parameter.
- **Outbound internet is limited to trusted domains.** BLS and DataUSA were both reachable from serverless on
  1 Nov 2026. If that changes, ingestion can run locally and upload with `--use-files-api`, though that path is
  untested.
- **Daily compute quota:** CI/CD deploys automatically but only runs the job on demand.

## Trade-offs: what I'd change for a client
- **Schema drift:** ingestion compares each file's column names and order with config before landing it, and any
  difference stops the run. That protects the tables, but one changed file also holds up every other source. A
  client pipeline might quarantine that file, alert and carry on.
- **Volume:** rebuilding transform tables each run is fine at this size. At scale I'd use change data capture
  (`AUTO CDC`) keyed on the file version and physically cluster the data.
- **Cost:** a daily schedule would be wasteful for a source that changes quarterly. I'd poll the listing cheaply
  and trigger the pipeline only on a change.
- **Access control:** separate schemas per layer would make grants simpler. I chose one schema for simplicity; a
  client would get read-only analyst access to `final_*` only, with a service principal running the job.
- **Monitoring:** alerts on job failure, expectation drop rates and source freshness, using the pipeline event log.
- **Manifest:** a JSON file is simple and works without Spark, but a single writer is assumed. The job sets
  `max_concurrent_runs: 1`. A Delta table would be better with several writers.
- **Secrets:** a shared team mailbox would be better than a personal contact.

## Retrospective
*To be written by me after the first deployed run: what was hardest to get right, and what surprised me.*

## AI usage
I used Claude Code to profile the data, draft the code, tests, deployment files and this document. I set the
architecture direction: raw/transform/final layers, metadata columns, config-driven ingestion, a shared Spark
utilities module, and wheel packaging. *Before submitting, update this with what I changed or fixed myself.*
