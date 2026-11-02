# Screenshot checklist

The reviewers may not be able to open the live workspace, so these screenshots are the runtime evidence. Take them
from the **`prod`** target (schema `workspace.rearc_quest`), whose resource names have no `dev` prefix. Save each
one in `docs/screenshots/` with the file name shown.

Before you start, run the job once more so every view shows the same run:
```bash
databricks bundle run quest_job -t prod
```

## Pipeline and job
| # | File name | Where | What must be visible |
|---|---|---|---|
| 1 | `01-job-run.png` | Jobs & Pipelines → `rearc_quest_job` → latest run | All three tasks (`ingest`, `run_pipeline`, `quality_checks`) green, with the run date |
| 2 | `02-pipeline-graph.png` | Jobs & Pipelines → `rearc_quest_pipeline` → latest update | The full graph from `raw_*` to `final_*`, status **Completed** |
| 3 | `03-pipeline-expectations.png` | Same update → select `transform_bls_data` → Data quality tab | The expectations (`series_id_not_null`, `year_not_null`, `valid_period`, `value_not_null`) with 0 failed records |
| 4 | `04-ingest-output.png` | Job run → `ingest` task → Output | The log line `Ingestion ... finished: {...}` |
| 5 | `05-quality-output.png` | Job run → `quality_checks` task → Output | `39 of 39 checks passed` |

## Volume and tables
| # | File name | Where | What must be visible |
|---|---|---|---|
| 6 | `06-volume-landing.png` | Catalog → `workspace` → `rearc_quest` → Volumes → `landing` → `raw/` | The dataset folders, and one opened to show an `_EXECUTION_PARTITION=...` folder |
| 7 | `07-tables-list.png` | Catalog → `workspace` → `rearc_quest` | The 24 tables: `raw_*`, `transform_*`, `final_*`, `quality_results` |
| 8 | `08-bronze-sample.png` | SQL editor: `SELECT * FROM workspace.rearc_quest.raw_bls_data LIMIT 10` | `raw_line` plus all six metadata columns |
| 9 | `09-silver-sample.png` | SQL editor: `SELECT * FROM workspace.rearc_quest.transform_bls_data LIMIT 10` | Typed columns (`series_id`, `year`, `period`, `value`) plus metadata |
| 10 | `10-gold-tables.png` | Catalog → the three `final_*` tables, or `SHOW TABLES LIKE 'final_*'` | `final_population_stats`, `final_series_best_year`, `final_series_population_report` |

## The three answers
Run the queries from the README's [Verifying the results](../README.md#verifying-the-results) section in the SQL
editor, after `USE CATALOG workspace; USE SCHEMA rearc_quest;`.

| # | File name | Query | What must be visible |
|---|---|---|---|
| 11 | `11-q1-population-stats.png` | `SELECT * FROM final_population_stats` | Mean and standard deviation for 2013-2018, with `years_included = 6` |
| 12 | `12-q2-best-year.png` | `SELECT series_id, series_label, best_year, summed_value, quarters_in_best_year, has_quarterly_data FROM final_series_best_year ORDER BY series_id` | Rows with readable labels; include `PRS30006032` if it fits |
| 13 | `13-q2-summary.png` | The count query from Gold 2 in the README | 282 series and 45 annual-only series |
| 14 | `14-q3-report.png` | `SELECT * FROM final_series_population_report ORDER BY year` | Years scrolled to show both NULL and filled-in populations |

## Optional
| # | File name | What |
|---|---|---|
| 15 | `15-quality-results.png` | `SELECT * FROM quality_results`, showing every check passed |
| 16 | `16-github-ci.png` | The GitHub Actions page showing CI passing |

## Before you commit them
- **Workspace details:** crop or blur the workspace URL and workspace ID in the browser bar if you don't want them
  public.
- **Email:** make sure your email isn't visible, for example in the account menu or in the `/Workspace/Users/...`
  paths shown on job and pipeline pages.
- **README link:** optionally, link the most important images from the README.
