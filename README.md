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
|   |-- gold_customer_metrics.sql
|   `-- gold_revenue_metrics.sql
|-- src/
|   |-- pipeline.py
|   |-- quality_checks.py
|   `-- sql_transforms.py
|-- tests/
|   |-- test_pipeline.py
|   |-- test_quality_checks.py
|   `-- test_sql_transforms.py
|-- .github/workflows/ci.yml
|-- .gitignore
`-- README.md
```

## Current Pipeline

1. Read raw retail orders from `data/raw/orders.csv`.
2. Write a bronze copy with minimal changes.
3. Build a silver dataset with cleaned types and valid rows.
4. Write rejected orders that were valid raw records but excluded from silver
   by configuration, with an explicit rejection reason for auditability.
5. Reconcile bronze rows against silver plus rejected rows so silent row loss
   or double-counting fails the run before downstream layers are written.
6. Write the silver layer as a flat CSV plus date-partitioned CSV and Parquet
   folders for incremental analytics reads.
7. Execute version-controlled SQL models to build gold revenue and customer summaries.
8. Run named data quality expectations, including a config-aware check that
   included order statuses match at least one source row, and persist their
   validation report.
9. Update an ingestion history keyed by source file checksum so repeated
   source files are visible even when they arrive under a different path.
10. Write a pipeline manifest with run timing and resolved paths, source
   checksum, source ingestion classification, config, row counts, quality
   status, row count reconciliation, source and silver data profiles,
   rejection reason counts, partition inventory, output artifact paths, and
   artifact size inventory for each run.

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

Pipeline paths and included order statuses are configured in
`config/pipeline.json`. The pipeline validates that paths are non-empty strings
and `included_statuses` is a non-empty list of unique, non-empty strings before
reading source data, so bad operational config fails fast instead of silently
rejecting every order. Relative `raw_path` and `processed_dir` values are
resolved from the project root, while absolute paths are preserved. That keeps
scheduled runs deterministic even when they start from a different working
directory. Pass `--config` to run the same executable entrypoint with an
environment-specific config file for CI, backfills, or scheduled jobs. Each run
emits progress logs for operation and troubleshooting.

Every push and pull request also runs the pipeline as a smoke test and executes
the full pytest suite in GitHub Actions. The integration test uses isolated
temporary input and verifies the generated bronze, silver, and gold datasets.
If a data quality expectation fails, the pipeline writes
`data_quality_report.json` before stopping so the failed run still has a
diagnostic artifact.

The gold layers are defined in `sql/gold_revenue_metrics.sql` and
`sql/gold_customer_metrics.sql`. The pipeline loads the cleaned silver rows into
an in-memory SQLite table and executes those models, so the SQL artifacts are
tested and used in every local and CI pipeline run.

Output files are written to:

```text
data/processed/
```

Each successful run also writes:

- `data_quality_report.json` with the overall validation status, source row
  count, and observed values for every expectation.
- `pipeline_manifest.json` with the UTC run timestamp, config path, resolved
  source and output paths, elapsed runtime, source file SHA-256, included order
  statuses, source ingestion classification (`new_source_file`,
  `repeated_source_file`, or `repeated_content_new_path`),
  bronze/silver/gold row counts, source status counts and order date range,
  silver customer/category/revenue profile, bronze-to-silver/rejected row count
  reconciliation, silver partition values, per-partition row counts and file
  paths, rejection reason counts, customer metric row counts, quality summary,
  generated artifact paths, and
  per-artifact existence/type/file-count/byte-size metadata.
- `ingestion_history.json` with every successfully processed source checksum,
  first/last seen timestamps, run count, row count, and known source paths.
  Failed quality or reconciliation runs do not update this history, which keeps
  it from certifying bad batches as processed.
- `rejected_orders.csv` with valid raw orders excluded from the silver layer by
  configured status and an explicit `rejection_reason`.
- `gold_customer_metrics.csv` with customer-level order count, units, revenue,
  and first/last order dates.
- `silver_orders_by_date/order_date=<YYYY-MM-DD>/silver_orders.csv` partition
  files for date-scoped silver reads.
- `silver_orders_by_date_parquet/order_date=<YYYY-MM-DD>/silver_orders.parquet`
  partition files for columnar analytics reads.

The pipeline currently checks that the dataset is non-empty, the raw schema
matches the expected order contract with no missing or unexpected named columns,
raw CSV rows are well formed with no missing or extra fields, order IDs are
populated and unique, quantity and price are positive numbers, order dates use
`YYYY-MM-DD`, key business dimensions are populated, and configured included
statuses match at least one source row before rows are partitioned or
aggregated.

## Roadmap

- Add PySpark version of the pipeline.
- [x] Add partitioned output.
- [x] Add Parquet writer for partitioned outputs.
- [x] Add Great Expectations style data quality checks.
- Add Airflow DAG for orchestration.
- [x] Add executable SQL model for gold transformations.
- Add dbt models and lineage for SQL transformations.
- [x] Add GitHub Actions for automated tests.
- [x] Add run manifest for pipeline observability.
- Add Power BI or Streamlit dashboard.
