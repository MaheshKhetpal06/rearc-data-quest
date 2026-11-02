# Rearc Data Quest: Databricks

A config-driven pipeline that lands BLS productivity files and US population data in a Unity Catalog Volume, then
models them as raw, transform and final tables with a Spark Declarative Pipeline. See [PROCESS.md](PROCESS.md) for
the reasoning.

## Layout
| Path | Purpose |
|---|---|
| `src/rearc_quest/config/dataset_config_definition.py` | All dataset and table config |
| `src/rearc_quest/utils/spark_utils.py` | Reusable Spark functions |
| `src/rearc_quest/ingestion/` | Source discovery, manifest, landing files |
| `src/rearc_quest/transforms/` | One method per `transform_method`, plus PySpark answers |
| `src/rearc_quest/sql/` | Spark SQL answers |
| `src/rearc_quest/quality/checks.py` | Post-pipeline checks |
| `src/rearc_quest/jobs/` | Wheel entry points: `rearc-ingest`, `rearc-quality` |
| `src/pipeline/definitions.py` | Registers pipeline tables from config |
| `databricks.yml` | Asset Bundle: schema, Volume, pipeline, job |
| `scripts/profile_sources.py` | One-time data profile |

## Local setup
No Java is needed.
```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev,spark]"
.venv/bin/pytest                      # Spark tests skip without Java or Databricks Connect
.venv/bin/ruff check .

cp .env.example .env                  # then set your contact email
set -a; source .env; set +a
.venv/bin/python scripts/profile_sources.py .sample
.venv/bin/rearc-ingest --volume-path .local_volume
```

### Spark tests without Java
Databricks Connect runs the Spark tests on serverless. It conflicts with plain `pyspark`, so it needs its own
environment. With Python 3.13, `databricks-connect` 16.1 requires NumPy 1.x, hence the PyArrow pin.
```bash
python3 -m venv .venv-connect
.venv-connect/bin/pip install databricks-connect "pyarrow==18.1.0" pandas pytest requests databricks-sdk
.venv-connect/bin/python -m pytest tests/test_spark.py   # uses the DEFAULT CLI profile
```

## Deploy to Databricks
```bash
brew tap databricks/tap && brew install databricks
databricks auth login --host <workspace-url>

databricks secrets create-scope rearc-quest
databricks secrets put-secret rearc-quest bls-contact-email --string-value "<your-email>"

databricks bundle validate
databricks bundle deploy
databricks bundle run quest_job
```
`python -m build` must be available to the deploying Python (`.venv/bin/activate` provides it).

If serverless can't reach BLS or DataUSA, land the files from your machine. Then run the pipeline and validation:
```bash
.venv/bin/rearc-ingest --use-files-api --volume-path /Volumes/workspace/<schema>/landing
```

## CI/CD
| Workflow | Trigger | Does |
|---|---|---|
| `ci.yml` | Pull request, push to `main` | Lint, all tests including Spark, build the wheel |
| `deploy.yml` | Push to `main`, manual | `bundle validate` and `deploy`, without running anything |
| `run_pipeline.yml` | Manual | Runs the job, to protect the Free Edition quota |

GitHub secrets needed: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.
