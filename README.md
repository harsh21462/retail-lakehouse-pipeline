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
├── data/
│   ├── raw/
│   │   └── orders.csv
│   └── processed/
├── sql/
│   └── gold_revenue_metrics.sql
├── src/
│   ├── pipeline.py
│   └── quality_checks.py
├── tests/
│   └── test_quality_checks.py
├── .gitignore
└── README.md
```

## Current Pipeline

1. Read raw retail orders from `data/raw/orders.csv`.
2. Write a bronze copy with minimal changes.
3. Build a silver dataset with cleaned types and valid rows.
4. Build a gold revenue summary by order date and category.
5. Run basic quality checks.

## Run Locally

```bash
python src/pipeline.py
```

Output files are written to:

```text
data/processed/
```

## Roadmap

- Add PySpark version of the pipeline.
- Add partitioned Parquet output.
- Add Great Expectations style data quality checks.
- Add Airflow DAG for orchestration.
- Add dbt models for SQL transformations.
- Add GitHub Actions for automated tests.
- Add Power BI or Streamlit dashboard.

## Daily Commit Ideas

- Day 1: Add project scaffold and sample data.
- Day 2: Add PySpark bronze-silver-gold pipeline.
- Day 3: Add data quality checks and tests.
- Day 4: Add SQL models for business metrics.
- Day 5: Add logging and config file.
- Day 6: Add Airflow DAG.
- Day 7: Add dashboard screenshots and architecture diagram.
