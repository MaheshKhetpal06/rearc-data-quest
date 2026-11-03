# Screenshots

Runtime evidence from the **`prod`** target on Databricks Free Edition (catalog `workspace`, schema `rearc_quest`),
captured on 2-3 Nov 2026. The job and log screenshots come from run `1064767987807863` on 3 Nov 2026.

## Job and pipeline
| Screenshot | Shows |
|---|---|
| [Job tasks](screenshots/screenshots/job-run-tasks.png) | `rearc_quest_job` deployed by the Asset Bundle: `ingest` → `run_pipeline` → `quality_checks` |
| [Job runs](screenshots/screenshots/job-run.png) | Successful runs of the job, with each task's status |
| [Pipeline graph](screenshots/screenshots/pipeline-graph.png) | `rearc_quest_pipeline`: the full graph from Volume sources to the `raw_*`, `transform_*` and `final_*` tables. All 23 tables completed, with expectations met |
| [Ingest log](screenshots/screenshots/ingest-output.png) | Ingestion log: secret resolved from the scope, 12 BLS files unchanged and skipped, population unchanged, run summary |
| [Pipeline expectations](screenshots/screenshots/pipeline_expectations.png) | `transform_bls_data`: 77,126 rows written, 0 dropped, all four expectations at 0% failed |
| [Quality check log](screenshots/screenshots/quality-output.png) | Per-table results, 39 results written, `39 of 39 checks passed` |
| [Quality results table](screenshots/screenshots/quality-results.png) | `quality_results`: 39 rows, all passed |

## Volume and tables
| Screenshot | Shows |
|---|---|
| [Landing Volume](screenshots/screenshots/volume-landing.png) | `landing` Volume: `_manifest/` and one `raw/` folder per dataset |
| [Tables](screenshots/screenshots/tables-list.png) | The 24 tables in `workspace.rearc_quest` |
| [Bronze sample](screenshots/screenshots/bronze_example_output.png) | `raw_bls_data`: raw lines with `_FILE_NAME`, `_SOURCE_URI`, `_EXECUTION_PARTITION` and `_RAW_DATA_HASH` |
| [Silver sample](screenshots/screenshots/silver_example_output.png) | `transform_bls_data`: typed columns plus metadata |
| [Gold tables](screenshots/screenshots/gold-example-output.png) | The three `final_*` tables |

## The three answers
| Screenshot | Shows |
|---|---|
| [Answer 1](screenshots/screenshots/population_stats.png) | Population 2013-2018: mean 322,069,808, sample standard deviation 4,158,441.04 |
| [Answer 2](screenshots/screenshots/q2_best_year.png) | Best year per series with readable labels (282 rows), including an annual-only series with NULLs |
| [Answer 3](screenshots/screenshots/q3-report.png) | `PRS30006032` Q01 by year, with population where available and NULL otherwise |

## CI
| Screenshot | Shows |
|---|---|
| [GitHub CI](screenshots/screenshots/github-ci.png) | CI passing on `main` |

To reproduce any of these in your own workspace, follow the [README runbook](../README.md#reviewer-runbook) and the
queries in [Verifying the results](../README.md#verifying-the-results).
