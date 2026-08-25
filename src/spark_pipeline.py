from pathlib import Path

try:
    from .pipeline import (
        load_config,
        raise_for_failed_reconciliation,
        resolve_pipeline_path,
    )
    from .quality_checks import REQUIRED_COLUMNS
except ImportError:  # Support direct execution with `python src/spark_pipeline.py`.
    from pipeline import (
        load_config,
        raise_for_failed_reconciliation,
        resolve_pipeline_path,
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


def run_spark_silver_pipeline(config_path):
    config_path = Path(config_path)
    config = load_config(config_path)
    raw_path = resolve_pipeline_path(config["raw_path"])
    processed_dir = resolve_pipeline_path(config["processed_dir"])
    SparkSession = _require_pyspark()

    spark = SparkSession.builder.appName("retail-lakehouse-silver").getOrCreate()
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
    silver_df.write.mode("overwrite").parquet(str(processed_dir / "spark_silver_orders"))
    rejected_df.write.mode("overwrite").parquet(
        str(processed_dir / "spark_rejected_orders")
    )
    return {
        "silver_path": str(processed_dir / "spark_silver_orders"),
        "rejected_path": str(processed_dir / "spark_rejected_orders"),
        "reconciliation": reconciliation,
    }


if __name__ == "__main__":
    run_spark_silver_pipeline(Path("config") / "pipeline.json")
