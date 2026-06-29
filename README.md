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
5. Write the silver layer as a flat CSV plus date-partitioned CSV and Parquet
   folders for incremental analytics reads.
6. Execute version-controlled SQL models to build gold revenue and customer summaries.
7. Run named data quality expectations and persist their validation report.
8. Write a pipeline manifest with source checksum, config, row counts, quality status, rejection reason counts, partition metadata, and output artifact paths for each run.

## Run Locally

```bash
python -m pip install -r requirements-dev.txt
python src/pipeline.py
python -m pytest -q
```

Pipeline paths and included order statuses are configured in
`config/pipeline.json`. Each run emits progress logs for operation and
troubleshooting.

Every push and pull request also runs the pipeline as a smoke test and executes
the full pytest suite in GitHub Actions. The integration test uses isolated
temporary input and verifies the generated bronze, silver, and gold datasets.

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
- `pipeline_manifest.json` with the UTC run timestamp, source file SHA-256,
  included order statuses, bronze/silver/gold row counts, silver partition
  values, rejection reason counts, customer metric row counts, quality summary,
  and generated artifact paths.
- `rejected_orders.csv` with valid raw orders excluded from the silver layer by
  configured status and an explicit `rejection_reason`.
- `gold_customer_metrics.csv` with customer-level order count, units, revenue,
  and first/last order dates.
- `silver_orders_by_date/order_date=<YYYY-MM-DD>/silver_orders.csv` partition
  files for date-scoped silver reads.
- `silver_orders_by_date_parquet/order_date=<YYYY-MM-DD>/silver_orders.parquet`
  partition files for columnar analytics reads.

The pipeline currently checks that the dataset is non-empty, required columns
exist, order IDs are unique, quantity and price are positive numbers, order
dates use `YYYY-MM-DD`, and key business dimensions are populated before rows
are partitioned or aggregated.

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

## Daily Commit Ideas

- Day 1: Add project scaffold and sample data.
- Day 2: Add PySpark bronze-silver-gold pipeline.
- Day 3: Add data quality checks and tests.
- Day 4: Add SQL models for business metrics.
- Day 5: Add logging and config file.
- Day 6: Add Airflow DAG.
- Day 7: Add dashboard screenshots and architecture diagram.
