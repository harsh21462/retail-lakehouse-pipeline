from datetime import datetime, timezone
import logging
from pathlib import Path
import time

try:
    from .pipeline import (
        build_runtime_environment,
        file_sha256,
        load_config,
        raise_for_failed_reconciliation,
        resolve_pipeline_path,
        write_json,
    )
    from .quality_checks import REQUIRED_COLUMNS
except ImportError:  # Support direct execution with `python src/spark_pipeline.py`.
    from pipeline import (
        build_runtime_environment,
        file_sha256,
        load_config,
        raise_for_failed_reconciliation,
        resolve_pipeline_path,
        write_json,
    )
    from quality_checks import REQUIRED_COLUMNS


SILVER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "category",
    "product",
    "quantity",
    "unit_price",
    "revenue",
]
REJECTED_COLUMNS = [*REQUIRED_COLUMNS, "rejection_reason"]
SPARK_MANIFEST_FILENAME = "spark_pipeline_manifest.json"
LOGGER = logging.getLogger(__name__)


def _require_pyspark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is required for src.spark_pipeline. Install pyspark in the "
            "runtime environment before running the Spark pipeline."
        ) from exc
    return SparkSession


def _require_pyspark_functions():
    try:
        from pyspark.sql import functions
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is required for src.spark_pipeline. Install pyspark in the "
            "runtime environment before running the Spark pipeline."
        ) from exc
    return functions


def _spark_sql_literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def build_silver_selection_sql(
    included_statuses,
    order_date_start=None,
    order_date_end=None,
):
    if not included_statuses:
        raise ValueError("included_statuses must contain at least one status")

    status_values = ", ".join(_spark_sql_literal(status) for status in included_statuses)
    predicates = [f"status in ({status_values})"]
    if order_date_start is not None:
        predicates.append(f"order_date >= {_spark_sql_literal(order_date_start)}")
    if order_date_end is not None:
        predicates.append(f"order_date <= {_spark_sql_literal(order_date_end)}")
    return " and ".join(predicates)


def build_rejection_reason_sql(
    included_statuses,
    order_date_start=None,
    order_date_end=None,
):
    status_values = ", ".join(_spark_sql_literal(status) for status in included_statuses)
    clauses = [
        f"when status not in ({status_values}) then 'status_not_included'",
    ]
    if order_date_start is not None:
        clauses.append(
            "when order_date < "
            f"{_spark_sql_literal(order_date_start)} then 'order_date_out_of_range'"
        )
    if order_date_end is not None:
        clauses.append(
            "when order_date > "
            f"{_spark_sql_literal(order_date_end)} then 'order_date_out_of_range'"
        )
    return "case " + " ".join(clauses) + " end"


def build_silver_and_rejected_dataframes(
    raw_orders_df,
    included_statuses,
    order_date_start=None,
    order_date_end=None,
):
    functions = _require_pyspark_functions()
    selection_sql = build_silver_selection_sql(
        included_statuses,
        order_date_start=order_date_start,
        order_date_end=order_date_end,
    )
    rejection_reason_sql = build_rejection_reason_sql(
        included_statuses,
        order_date_start=order_date_start,
        order_date_end=order_date_end,
    )

    silver_df = raw_orders_df.where(selection_sql).selectExpr(
        "order_id",
        "customer_id",
        "order_date",
        "category",
        "product",
        "cast(quantity as int) as quantity",
        "cast(unit_price as double) as unit_price",
        "cast(quantity as int) * cast(unit_price as double) as revenue",
    )
    rejected_df = (
        raw_orders_df.where(f"not ({selection_sql})")
        .withColumn("rejection_reason", functions.expr(rejection_reason_sql))
        .select(*REJECTED_COLUMNS)
    )
    return silver_df, rejected_df


def build_spark_row_count_reconciliation(raw_orders_df, silver_df, rejected_df):
    bronze_count = raw_orders_df.count()
    silver_count = silver_df.count()
    rejected_count = rejected_df.count()
    accounted_count = silver_count + rejected_count
    return {
        "success": bronze_count == accounted_count,
        "bronze_rows": bronze_count,
        "silver_rows": silver_count,
        "rejected_rows": rejected_count,
        "accounted_rows": accounted_count,
        "difference": bronze_count - accounted_count,
    }


def build_spark_manifest(
    *,
    config_path,
    config,
    raw_path,
    processed_dir,
    started_at_utc,
    completed_at_utc,
    duration_ms,
    reconciliation,
):
    return {
        "version": 1,
        "engine": "spark",
        "run": {
            "config_path": str(Path(config_path).resolve()),
            "config_sha256": file_sha256(config_path),
            "raw_path": str(raw_path),
            "processed_dir": str(processed_dir),
            "started_at_utc": started_at_utc,
            "completed_at_utc": completed_at_utc,
            "duration_ms": duration_ms,
        },
        "runtime_environment": build_runtime_environment(),
        "source": {
            "path": str(raw_path),
            "sha256": file_sha256(raw_path),
            "rows": reconciliation["bronze_rows"],
        },
        "config": {
            "included_statuses": list(config["included_statuses"]),
            "order_date_window": {
                "start": config.get("order_date_start"),
                "end": config.get("order_date_end"),
            },
        },
        "outputs": {
            "silver_orders": {
                "path": str(processed_dir / "spark_silver_orders"),
                "format": "parquet",
                "columns": list(SILVER_COLUMNS),
                "rows": reconciliation["silver_rows"],
            },
            "rejected_orders": {
                "path": str(processed_dir / "spark_rejected_orders"),
                "format": "parquet",
                "columns": list(REJECTED_COLUMNS),
                "rows": reconciliation["rejected_rows"],
            },
        },
        "reconciliation": reconciliation,
    }


def run_spark_silver_pipeline(config_path):
    started_at = datetime.now(timezone.utc)
    started_at_monotonic = time.perf_counter()
    config_path = Path(config_path)
    config = load_config(config_path)
    raw_path = resolve_pipeline_path(config["raw_path"])
    processed_dir = resolve_pipeline_path(config["processed_dir"])
    SparkSession = _require_pyspark()

    spark = SparkSession.builder.appName("retail-lakehouse-silver").getOrCreate()
    try:
        raw_orders_df = spark.read.option("header", True).csv(str(raw_path))
        silver_df, rejected_df = build_silver_and_rejected_dataframes(
            raw_orders_df,
            config["included_statuses"],
            order_date_start=config.get("order_date_start"),
            order_date_end=config.get("order_date_end"),
        )
        reconciliation = build_spark_row_count_reconciliation(
            raw_orders_df,
            silver_df,
            rejected_df,
        )
        raise_for_failed_reconciliation(reconciliation)
        silver_path = processed_dir / "spark_silver_orders"
        rejected_path = processed_dir / "spark_rejected_orders"
        silver_df.write.mode("overwrite").parquet(str(silver_path))
        rejected_df.write.mode("overwrite").parquet(str(rejected_path))
        completed_at_utc = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        manifest = build_spark_manifest(
            config_path=config_path,
            config=config,
            raw_path=raw_path,
            processed_dir=processed_dir,
            started_at_utc=started_at.isoformat().replace("+00:00", "Z"),
            completed_at_utc=completed_at_utc,
            duration_ms=round((time.perf_counter() - started_at_monotonic) * 1000, 3),
            reconciliation=reconciliation,
        )
        manifest_path = processed_dir / SPARK_MANIFEST_FILENAME
        write_json(manifest_path, manifest)
        return {
            "silver_path": str(silver_path),
            "rejected_path": str(rejected_path),
            "manifest_path": str(manifest_path),
            "reconciliation": reconciliation,
        }
    finally:
        stop = getattr(spark, "stop", None)
        if stop is not None:
            try:
                stop()
            except Exception:  # pragma: no cover - defensive cleanup logging.
                LOGGER.warning("Failed to stop Spark session", exc_info=True)


if __name__ == "__main__":
    run_spark_silver_pipeline(Path("config") / "pipeline.json")
