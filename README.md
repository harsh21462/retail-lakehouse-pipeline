# Retail Lakehouse Pipeline

A data engineering project that simulates a retail analytics pipeline using a bronze-silver-gold lakehouse pattern.

The goal is to ingest raw order data, clean and validate it, build analytics-ready tables, and expose business metrics such as revenue, order volume, average order value, and category performance.

## Why This Project

This project is designed to demonstrate practical data engineering skills:

- Batch ingestion from raw CSV files.
- Bronze, silver, and gold data layers.
- Data quality checks for nulls, duplicates, and invalid values.
- Transformations using Python and SQL.
- Analytics-ready outputs for reporting.
- Clear structure for future Spark, Airflow, dbt, and cloud upgrades.

## Project Structure

```text
retail-lakehouse-pipeline/
|-- config/
|   `-- pipeline.json
|-- data/
|   |-- raw/
|   |   `-- orders.csv
|   `-- processed/
|-- sql/
|   |-- gold_category_metrics.sql
|   |-- gold_customer_metrics.sql
|   |-- gold_rejection_metrics.sql
|   `-- gold_revenue_metrics.sql
|-- dags/
|   `-- retail_lakehouse_dag.py
|-- dbt/
|   |-- dbt_project.yml
|   `-- models/
|       |-- sources.yml
|       `-- gold/
|-- src/
|   |-- pipeline.py
|   |-- quality_checks.py
|   |-- spark_pipeline.py
|   |-- streamlit_dashboard.py
|   `-- sql_transforms.py
|-- tests/
|   |-- test_dbt_models.py
|   |-- test_pipeline.py
|   |-- test_quality_checks.py
|   |-- test_spark_pipeline.py
|   |-- test_streamlit_dashboard.py
|   `-- test_sql_transforms.py
|-- .github/workflows/ci.yml
|-- .gitignore
`-- README.md
```

## Current Pipeline

1. Read raw retail orders from `data/raw/orders.csv`.
2. Write a bronze copy with minimal changes.
3. Build a silver dataset with cleaned types and valid rows, optionally scoped
   to a configured order-date window for backfills or incremental runs.
4. Write rejected orders that were valid raw records but excluded from silver
   by configuration, with an explicit rejection reason for auditability.
5. Reconcile bronze rows against silver plus rejected rows so silent row loss
   or double-counting fails the run before downstream layers are written.
6. Write the silver layer as a flat CSV plus date-partitioned CSV and Parquet
   folders for incremental analytics reads.
7. Execute version-controlled SQL models to build gold revenue, customer,
   category, and rejection-impact summaries, validating each model's output
   columns before gold CSVs are published. The `dbt/` project mirrors those
   gold transformations as dbt-compatible models with source and column tests
   for teams that want to run the same analytics layer in a warehouse.
8. Run named data quality expectations, including config-aware checks that
   included order statuses and any configured order-date window match at least
   one source row, and persist their validation report.
9. Update an ingestion history keyed by source file checksum so repeated
   source files are visible even when they arrive under a different path.
10. Write a pipeline manifest with run timing and resolved paths, source
    checksum, source ingestion classification, config path and checksum,
    config, row counts, quality status, row count reconciliation, source and
    silver data profiles with deterministic high-watermarks,
    rejection reason counts, bounded rejected-order samples by reason,
    accepted and rejected business-impact metrics,
    non-blocking health warnings, run-to-run comparison
    against the previous manifest, including business-impact, source
    status-count, and rejection-reason deltas, partition inventory, output
    artifact paths,
    artifact size and checksum inventory, and a machine-readable
    runtime environment snapshot, schema contract,
    published-artifact schema contract validation, SQL model
    inventory, published CSV data type validation, partitioned CSV validation,
    partitioned Parquet schema validation, gold metric reconciliation,
    lineage graph, and deterministic
    artifact checksums for each run. Health warning breaches are also
    normalized into machine-readable observed-versus-threshold records so
    schedulers and dashboards do not need to parse warning text.
11. Write a Markdown run summary derived from the manifest for quick
    operational review of source ingestion status, quality expectation
    pass/fail details, warning counts, row-count and business-impact deltas,
    source and silver high-watermarks, accepted versus rejected revenue
    exposure, sampled rejected orders by reason, and changed artifacts.
12. Write a generated data catalog from the published schema contracts,
    output paths, row counts, and silver partition inventory so BI consumers
    have a versioned handoff without reverse-engineering the manifest.
13. Write a dependency-free `dashboard.html` with run health, accepted and
    rejected revenue KPIs and deltas, row-count bars, status mix, and rejection
    reasons for portfolio review or lightweight operational handoff.
14. Optionally explore the latest manifest through a Streamlit dashboard for
   interactive portfolio demos without changing the batch pipeline contract.
15. Optionally schedule the same CLI entrypoint through the checked-in Airflow
   DAG in `dags/retail_lakehouse_dag.py`.
16. Optionally run the Spark silver-layer adapter in `src/spark_pipeline.py`
   when PySpark is installed. It uses the same config, status scope, date
   window semantics, silver columns, rejected-order columns, and rejection
   reasons as the Python pipeline, reconciles raw rows against Spark silver
   plus rejected rows before overwriting outputs, and writes Spark-managed
   Parquet outputs under `spark_silver_orders/` and `spark_rejected_orders/`.
   Each successful Spark run also writes a `spark_pipeline_manifest.json`
   with run timing, source/config checksums, resolved output paths, runtime
   environment details, row counts, and the Spark reconciliation result.

CSV and JSON artifacts are written through same-directory temporary files and
atomically replaced when the write succeeds, so a failed run does not leave
half-written metadata or table files for downstream readers.
Partitioned CSV and Parquet directories are also built in same-parent staging
directories and swapped into place only after every partition file is written,
preserving the previous complete partition set if a partition write fails.

## Run Locally

```bash
python -m pip install -r requirements-dev.txt
python src/pipeline.py --config config/pipeline.json
python -m pytest -q
```

Pipeline paths, included order statuses, and optional order date bounds are
configured in `config/pipeline.json`. The pipeline validates that paths are
non-empty strings and `included_statuses` is a non-empty list of unique,
non-empty strings before reading source data. Optional `order_date_start` and
`order_date_end` values must use `YYYY-MM-DD`, and the start must be on or
before the end. Optional `warning_thresholds` can set `max_rejection_rate`
between 0 and 1, `min_silver_rows` as a non-negative integer, and
`max_source_lag_days` as a non-negative integer freshness SLA against the
latest source order date. It can also set `max_future_order_date_days` as a
non-negative integer tolerance for future-dated source rows, which catches
clock, timezone, or upstream export mistakes without blocking controlled
forward-looking loads. Threshold breaches are written to the manifest as health
warnings but do not fail the run; malformed threshold config fails fast.
Bad operational config fails fast instead of silently rejecting every order.
Relative `raw_path` and
`processed_dir` values are
resolved from the project root, while absolute paths are preserved. That keeps
scheduled runs deterministic even when they start from a different working
directory. Pass `--config` to run the same executable entrypoint with an
environment-specific config file for CI, backfills, or scheduled jobs. Each run
emits progress logs for operation and troubleshooting.

Example windowed backfill config:

```json
{
  "raw_path": "data/raw/orders.csv",
  "processed_dir": "data/processed",
  "included_statuses": ["delivered"],
  "order_date_start": "2026-06-01",
  "order_date_end": "2026-06-30",
  "warning_thresholds": {
    "max_rejection_rate": 0.2,
    "max_future_order_date_days": 1,
    "max_source_lag_days": 2,
    "min_silver_rows": 1000
  }
}
```

Every push and pull request also runs the pipeline as a smoke test and executes
the full pytest suite in GitHub Actions. The integration test uses isolated
temporary input and verifies the generated bronze, silver, and gold datasets.
If a data quality expectation fails, the pipeline writes
`data_quality_report.json` and `pipeline_run_summary.md` before stopping so
the failed run still has diagnostic artifacts. The report includes bounded
row-level failure samples with the affected expectations and raw source
values, capped at 10 rows so a bad batch does not create an unbounded
diagnostic artifact. The Markdown summary includes the failed expectation
names and sampled row identifiers for quick triage without opening JSON.

The gold layers are defined in `sql/gold_revenue_metrics.sql`,
`sql/gold_customer_metrics.sql`, `sql/gold_category_metrics.sql`, and
`sql/gold_rejection_metrics.sql`. The pipeline loads the cleaned silver rows
and rejected-order rows into in-memory SQLite tables and executes those models,
so the SQL artifacts are tested and used in every local and CI pipeline run.
Each gold model has an explicit output-column contract in the Python pipeline;
renamed, missing, or extra SQL output columns fail fast before downstream CSV
artifacts are replaced.

## dbt Models

The repository also includes a dbt project under `dbt/` that mirrors the
pipeline's gold SQL layer:

- `dbt/models/sources.yml` declares the published `silver_orders` and
  `rejected_orders` lakehouse tables as dbt sources with basic not-null and
  uniqueness tests.
- `dbt/models/gold/gold_revenue_metrics.sql`,
  `gold_customer_metrics.sql`, `gold_category_metrics.sql`, and
  `gold_rejection_metrics.sql` define warehouse-ready gold models using dbt
  `source()` lineage.
- `dbt/models/gold/schema.yml` documents the model contracts and column-level
  tests.

Local CI does not require a dbt adapter. Instead, `tests/test_dbt_models.py`
keeps the dbt model inventory aligned with the executable Python pipeline's
SQL model registry. Install dbt and a warehouse adapter separately before
running `dbt build` against a real database.

## Airflow Scheduling

`dags/retail_lakehouse_dag.py` defines a daily Airflow DAG named
`retail_lakehouse_daily`. It calls the same CLI entrypoint used locally:

```bash
python src/pipeline.py --config config/pipeline.json
```

The DAG file is safe to import in environments where Airflow is not installed,
which keeps local pytest and lightweight CI jobs from depending on an Airflow
runtime. In an Airflow deployment, set `RETAIL_LAKEHOUSE_PROJECT_ROOT` if the
repository is mounted somewhere other than the DAG file's parent project
directory, and set `RETAIL_LAKEHOUSE_PYTHON_BIN` if the scheduler should use a
specific virtualenv interpreter.

## PySpark Silver Adapter

`src/spark_pipeline.py` provides an optional Spark implementation for the
silver and rejected-order transform boundary:

```bash
python -m pip install pyspark
python src/spark_pipeline.py
```

PySpark is intentionally not part of `requirements-dev.txt` because it brings a
JVM/runtime dependency that is unnecessary for the default local and CI checks.
The adapter fails with an explicit error when PySpark is missing, and the unit
tests validate the generated Spark predicates, status/date-window handling, and
published column contracts without requiring a Spark runtime. Before publishing
Spark-managed Parquet outputs, it also checks that the raw row count equals
accepted plus rejected rows and fails the run if Spark did not account for every
input row. Successful Spark runs emit `spark_pipeline_manifest.json` next to the
Spark Parquet outputs, and the Spark session is stopped in a `finally` block so
failed reconciliations do not leak a live session in scheduled environments.

Output files are written to:

```text
data/processed/
```

Each successful run also writes:

- `data_quality_report.json` with the overall validation status, source row
  count, a pass/fail expectation summary, and observed values for every
  expectation, plus bounded failed-row samples for quick triage.
- `pipeline_manifest.json` with the UTC run timestamp, config path, resolved
  source and output paths, elapsed runtime, config file SHA-256, source file
  SHA-256, included order statuses, optional order-date window, source ingestion
  classification
  (`new_source_file`, `repeated_source_file`, or `repeated_content_new_path`),
  bronze/silver/gold row counts, source status counts, source and silver order
  date ranges, high-watermarks for the latest processed order date and order
  ID, silver customer/category/revenue profile, bronze-to-silver/rejected row
  count reconciliation, accepted and rejected order, unit, revenue, rejection
  rate, realized revenue rate, and average order value metrics, configured
  health warning thresholds and any warning breaches, silver partition values,
  per-partition row counts and file paths,
  rejection reason counts, bounded rejected-order samples grouped by reason,
  customer, category, and rejection metric row counts,
  quality summary with failed expectation names and bounded failed-row sample
  metadata, health status, warning count, machine-readable threshold breach
  records with observed values and configured thresholds, generated artifact
  paths, runtime environment metadata for the
  Python implementation, executable, platform, selected dependency versions,
  and scheduler environment variables, schema contracts for published
  bronze, silver, rejected-order, and gold outputs, contract validation results
  proving the emitted CSV headers still match those declared schemas,
  per-layer CSV data type validation proving emitted values still conform to
  declared date, integer, and float contracts, partitioned CSV validation
  proving each physical partition file still uses the declared silver schema
  and contains only rows for its directory's `order_date` value,
  partitioned Parquet contract validation proving each physical partition file
  still matches the declared silver schema excluding the directory partition
  column, metric reconciliation proving gold order, unit, and revenue totals
  still tie back to the silver and rejected-order source tables, and
  per-artifact existence/type/file-count/byte-size/SHA-256 metadata.
  File
  artifacts are hashed directly, while directory artifacts are hashed from
  sorted relative file paths and file contents so partitioned outputs can be
  compared across runs. The run comparison also records whether source and
  silver high-watermarks changed since the previous manifest, plus raw source
  status-count deltas and business-impact deltas so changes in order status
  mix and revenue exposure are visible before they flow into rejection metrics.
  The manifest also includes a versioned inventory of
  executable SQL gold models with model paths, SHA-256 checksums, input tables,
  output artifacts, and output-column contracts. The manifest also includes a
  versioned lineage graph linking the raw source, quality report,
  ingestion history, bronze layer, silver layer, partitioned silver outputs,
  rejected-order audit table, and executable SQL gold models. When a previous
  manifest exists, `run_comparison` records layer row-count deltas, source and
  config checksum changes, quality status changes, warning-count deltas, source
  status-count deltas, rejection-reason count deltas, and per-artifact
  existence/checksum changes, plus runtime-environment and dependency-version
  drift;
  if a prior manifest is missing or malformed, the comparison records the
  reason instead of fabricating deltas. Business-impact deltas cover accepted
  and rejected orders, rejection rate, accepted revenue, rejected potential
  revenue, and realized revenue rate.
- `pipeline_run_summary.md` with a concise human-readable handoff of source
  ingestion classification, quality status, per-expectation pass/fail
  observations, failed-row sample identifiers when quality checks fail,
  health warnings, threshold breach details, current row counts, run-to-run
  row-count and business-impact deltas, source and silver high-watermarks,
  accepted versus rejected revenue exposure, sampled rejected orders by reason,
  and changed artifacts from the manifest.
- `data_catalog.md` with a BI-friendly catalog of published bronze, silver,
  rejected-order, and gold artifacts, their row counts, descriptions, column
  names, declared data types, and available silver CSV/Parquet partitions.
- `dashboard.html` with a self-contained visual dashboard for run health,
  quality status, threshold breaches, business-impact KPIs and deltas, layer
  row counts, source status mix, rejected-order reasons, and configured run
  scope.
- `ingestion_history.json` with every successfully processed source checksum,
  first/last seen timestamps, run count, row count, and known source paths.
  Failed quality or reconciliation runs do not update this history, which keeps
  it from certifying bad batches as processed.
- `rejected_orders.csv` with valid raw orders excluded from the silver layer by
  configured status or order-date window and an explicit `rejection_reason`.
- `gold_customer_metrics.csv` with customer-level order count, units, revenue,
  and first/last order dates.
- `gold_category_metrics.csv` with category-level order count, customer count,
  units, revenue, average order value, and first/last order dates.
- `gold_rejection_metrics.csv` with rejected order counts, units, and potential
  revenue grouped by rejection reason, source status, order date, and category.
- `silver_orders_by_date/order_date=<YYYY-MM-DD>/silver_orders.csv` partition
  files for date-scoped silver reads.
- `silver_orders_by_date_parquet/order_date=<YYYY-MM-DD>/silver_orders.parquet`
  partition files for columnar analytics reads.

## Streamlit Dashboard

The optional Streamlit dashboard reads the same `pipeline_manifest.json` as the
static HTML dashboard and adds an interactive view of run health, quality
status, business-impact KPIs and deltas, layer row counts, rejection reasons,
threshold breaches, and artifact changes.

Streamlit is intentionally not required by the batch pipeline or CI test suite.
Install it only when you want to run the interactive UI:

```bash
python -m pip install streamlit
streamlit run src/streamlit_dashboard.py
```

To inspect a non-default run artifact:

```bash
streamlit run src/streamlit_dashboard.py -- --manifest data/processed/pipeline_manifest.json
```

The pipeline currently checks that the dataset is non-empty, the raw schema
matches the expected order contract with no missing or unexpected named columns,
raw CSV rows are well formed with no missing or extra fields, order IDs are
populated and unique, quantity and price are positive numbers, order dates use
`YYYY-MM-DD`, key business dimensions are populated, and configured included
statuses plus any configured order-date window match at least one source row
before rows are partitioned or aggregated.

## Roadmap

- [x] Add PySpark silver-layer adapter.
- Add full PySpark orchestration for gold models and manifest parity.
- [x] Add partitioned output.
- [x] Add Parquet writer for partitioned outputs.
- [x] Add Great Expectations style data quality checks.
- [x] Add Airflow DAG for orchestration.
- [x] Add executable SQL model for gold transformations.
- [x] Add manifest-native lineage for source, lakehouse layers, and gold models.
- [x] Add dbt models and lineage for SQL transformations.
- [x] Add GitHub Actions for automated tests.
- [x] Add run manifest for pipeline observability.
- [x] Add lightweight dashboard artifact.
- [x] Add Power BI or Streamlit dashboard.
