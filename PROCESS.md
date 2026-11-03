# Process

> **Status:** deployed with an Asset Bundle to Databricks Free Edition (`dev` and `prod` targets), both manually
> and through GitHub Actions, and run end to end on 1-3 Nov 2026. Screenshots from `prod` are indexed in
> [docs/SCREENSHOTS.md](docs/SCREENSHOTS.md). The [README](README.md) has the reviewer runbook.

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
is easy for an analyst to review. The PySpark versions in `transforms/final_tables.py` are the documented
alternative, and `primary_language` in config switches between them. Everything around the answers is PySpark,
because looping over config is much easier in Python.

An earlier version of the job compared both versions with `exceptAll` in both directions after every run. On the
full data, all three answers matched on `dev` (1 Nov 2026) and `prod` (2 Nov 2026). I then removed that comparison
and its unit test, because the two versions implement the same logic and the check added a job step without adding
much. The PySpark versions are no longer executed by any job or test. Changing `primary_language` to `pyspark` and
running the pipeline would exercise them.

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

**Data quality.** Ingestion rejects a file whose column names or order differ from config. Expectations fail on an
unexpected field count at raw, drop rows with null keys or bad types at transform, warn on missing lookup labels,
and fail on an invalid final table. `try_cast` turns bad values into NULLs so expectations can count them instead of
the run crashing. `quality/checks.py` adds simple checks after the pipeline. Results are saved to `quality_results`.

**A job and a pipeline, and why both.** The pipeline (`rearc_quest_pipeline`) only builds tables: it reads files
already in the Volume and produces the 23 `raw_*`, `transform_*` and `final_*` tables, with expectations and
lineage. Downloading from BLS and DataUSA isn't something a pipeline should do: it's ordinary Python with HTTP
requests, a manifest and writes to the Volume, and pipeline dataset functions must not perform writes or other side
effects. Writing `quality_results` is also a table write the pipeline can't make. So the job (`rearc_quest_job`)
runs three tasks in order:
1. `ingest`, the wheel entry point `rearc-ingest`, which lands files
2. `run_pipeline`, which builds the tables
3. `quality_checks`, the wheel entry point `rearc-quality`, which checks the results

If a task fails, the later ones are skipped. The job is also the one thing to schedule or start from CI/CD. The
pipeline can still run alone to rebuild tables from what's already landed.

**Logging.** Both wheel entry points use Python's standard `logging`, so the logs appear in each task's output in
the job run:
- **Ingestion:** run start, each source's discovery counts, each downloaded file's outcome with its dataset, size
  and partition, removed files as warnings, every HTTP request with status, size and duration, where the BLS contact
  came from, failures with the source and totals so far, and a one-line run summary. The run ID is the execution
  partition, which ties the log to the landing folders.
- **Quality checks:** one line per table and a run summary.
- **Never logged:** credentials, the contact email and HTTP headers, because the User-Agent contains the contact.

Per-table pipeline progress and expectation results are in the pipeline's event log rather than in Python logs,
because dataset functions are evaluated repeatedly and shouldn't log.

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
- **Quality history:** `quality_results` is overwritten each run, so only the latest results are kept. Appending
  with a run timestamp would give a trend.
- **Run correlation:** logs use the ingestion run's partition as the run ID. Passing the Databricks job run ID into
  the tasks would tie logs, tables and job runs together directly.

## To-dos before production
With more time, I would have added these before calling this production-ready:

- **Alerting**
  - Notify on job or pipeline failure, quality check failures, and expectation drop rates above a threshold.
  - Alert on source freshness, for example BLS not publishing when expected, or a `SourceSchemaError`.
  - Route alerts to a team channel rather than a personal inbox.
- **Lineage tracking**
  - Unity Catalog already records table-level lineage for the pipeline.
  - I'd extend it end to end, so each Gold row can be traced back to its source file, landing partition and job run.
    That means passing the Databricks job run ID into every task and log, and checking column-level lineage from
    Bronze to Gold.
- **Dashboarding**
  - An AI/BI dashboard on the Gold tables for the three answers.
  - Operational views: run history, durations, file outcomes per run, expectation pass rates and quality results
    over time. That needs `quality_results` to keep history rather than only the latest run.
- **Semantic layer for AI and Genie**
  - Describe the Gold tables so an AI agent can answer plain-English questions through a Genie space:
    - table and column comments
    - business definitions, for example best year, sample standard deviation, and what each series label means
    - metric definitions
    - example questions with their trusted SQL
  - Grant a read-only role on the Gold layer only, so the agent queries curated data, never raw files.

**Known gaps in the current code**
- **Unused config fields:** `DatasetConfig.schema_name` and `final_table_name` aren't read anywhere. Tables go to
  the bundle's schema, and `FINAL_TABLES` drives the Gold tables.
- **Secret lookup hides errors:** `get_secret` treats any error reaching Databricks as a missing secret, which can
  hide an authentication or network problem.
- **Untested paths:** the failure paths of the new logging, `bundle destroy`, and the local Files API fallback
  haven't been run.

## Requirement coverage
| Requirement | Status | Evidence |
|---|---|---|
| Land the full BLS `pr` folder in a Volume | Complete, runtime-verified | All 12 files landed on `dev` and `prod`, including `pr.txt` and `pr.contacts` |
| Resolve the BLS 403 the way BLS asks | Complete, runtime-verified | Contact in the User-Agent, read from a Databricks secret |
| No hardcoded filenames | Complete | Files come from the listing and are routed by pattern (`route_file`), with unknown files landing under `unmapped/` |
| Re-runs handle added, changed and removed data | Complete, runtime-verified | Unit tests, plus a simulated change, removal and addition on `dev` (1 Nov 2026) that the transform tables reflected correctly |
| Re-runs don't reprocess | Complete, runtime-verified | Repeat runs report `{'unchanged': 13}` and land nothing |
| Population API saved as JSON in the same Volume | Complete, runtime-verified | `raw/population/.../population.json` |
| Bronze, Silver and Gold as a Spark Declarative Pipeline | Complete, runtime-verified | 10 raw, 10 transform and 3 final tables |
| Expectations: expected schema, non-null keys | Complete, runtime-verified | Field-count and non-null expectations, 0 failures on `prod`. Column names and order are checked at ingestion, not as an expectation |
| Each answer in Spark SQL and in PySpark | Implemented | SQL feeds Gold. PySpark matched it on full data on 1-2 Nov 2026, but nothing executes it now (see above) |
| Gold answers correct | Runtime-verified | Values in the README, cross-checked against a pure-Python profile and DuckDB |
| Human-readable series labels | Complete, runtime-verified | `series_label` from the sector, measure, duration and class lookups |
| Read the provider's docs | Complete | `pr.txt` (Q05, file formats) and the DataUSA API docs (address, 1-year vs 5-year) |
| Public repository, PROCESS.md | Complete | |
| Screenshots | Complete | 16 screenshots from `prod`, indexed in `docs/SCREENSHOTS.md` |
| Bonus: deploy with Asset Bundles | Implemented, runtime-verified | `databricks.yml`, deployed to `dev` and `prod`, including through GitHub Actions |
| Bonus: Genie space or dashboard | Not implemented | |
| Bonus: Unity Catalog access controls | Not implemented | Described under trade-offs only |
| Extra: CI, quality job, metadata columns | Implemented | CI ran 63 tests on GitHub, including Spark; the quality job reports 39 of 39 checks |
| Extra: CD and operational logging | Implemented, runtime-verified | A push to `main` deployed `prod` through GitHub Actions on 3 Nov 2026, and the following `prod` run showed the new logs |

## Retrospective
This is a record of what actually caused difficulty, taken from the project history.

- **The data was the subtle part, not the code.** Three traps would each have given a plausible but wrong answer
  without an error:
  - `Q05` mixed into the quarterly sums
  - `pr.data.0.Current` loaded alongside `AllData`
  - padded headers and fields breaking joins

  Profiling the files before modeling found all three, and two contradicted or went beyond the documentation.
  `pr.txt` describes `Current` as year-to-date, but it goes back to 1995.
- **Change handling with append-only raw tables.** Auto Loader only appends, so changed and removed files needed a
  separate record of which file version is current. The answer was immutable landing folders plus a manifest that the
  transform layer joins against. It was confirmed on Databricks with a simulated change, removal and new file.
- **Packaging the wheel for the pipeline.** The first deployment failed with
  `ModuleNotFoundError: No module named 'rearc_quest'`. The pipeline's editable install of the project didn't work
  on serverless, and attaching the built wheel, as the job does, fixed it.
- **Tooling around the work.** Getting the GitHub token's permissions right (contents, then workflows, then
  actions) and keeping a personal email out of commit history took more time than expected.

## AI usage
I used Claude Code (Anthropic) throughout this project. To be clear about who did what:

**Decisions I made**
- **Layers:** raw, transform and final, mapped to Bronze, Silver and Gold.
- **Metadata columns in every layer:** `_EXECUTION_PARTITION`, `_RAW_DATA_HASH`, `_INGESTED_AT` and
  `_PROCESSED_AT`, later adding `_SOURCE_URI` and `_FILE_NAME`.
- **Config-driven ingestion:** one `dataset_config_definition.py` holding each dataset's schema, table names,
  transform method, source and credential.
- **Code structure:** reusable Spark utilities in one `spark_utils.py`, called by transform methods.
- **Packaging and deployment:** a wheel deployed with an Asset Bundle, with no notebooks.
- **Local setup:** a Python virtual environment only, with no Java.
- **Data quality:** simple PySpark checks rather than an external tool.
- **Profiling first:** profile the data with a one-time download before creating the repo.
- **API address:** switch the population API to the address in DataUSA's documentation.
- **Removing the equivalence check:** drop the SQL-vs-PySpark comparison job.
- **Header checks:** add a check of source column names.
- **Logging:** add operational logging only, with no behaviour changes, and test it by deploying through CD rather
  than by hand.
- **Order of steps:** deploy to `prod` before taking screenshots.

**What Claude Code did**
- **Research:** read the assignment, the BLS documentation and the Databricks documentation, and checked the data
  sources.
- **Profiling:** wrote and ran the profiling script, and summarised the data findings.
- **Code:** wrote the code, tests, bundle configuration, CI workflows, the logging and drafts of these documents.
- **Running it:** ran the deployments, jobs, pipeline updates and SQL checks through the Databricks CLI.
- **Fixes:** diagnosed and fixed the wheel import failure, and the deployment configuration issues the CLI reported.
- **Explanations:** explained the design, including the best-year SQL and ingestion end to end, so I could review
  it.

**Validation, and who ran it**
- **Run by me:**
  - installing and logging in to the Databricks CLI
  - inspecting the secret scope
  - creating the GitHub repository and token
  - pushing and rewriting the commit history
  - adding the GitHub Actions secrets and starting the Deploy workflow, which deployed `prod` successfully
  - capturing the screenshots in `docs/SCREENSHOTS.md` from the `prod` workspace
  - reviewing the results reported back to me
- **Run by Claude Code at my request:**
  - unit tests: locally, through Databricks Connect, and in CI on GitHub
  - local and Databricks ingestion, including the repeat-run and simulated-change tests
  - full job runs on `dev` and `prod`, including the 3 Nov `prod` run after CD deployed the logging
  - SQL queries confirming the answers, the row counts and the expectation metrics
  - checks that the screenshots came from `prod` and showed no personal details


