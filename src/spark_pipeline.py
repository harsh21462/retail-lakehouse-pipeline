import argparse
from datetime import datetime, timezone
import logging
from pathlib import Path
import shutil
import time

try:
    from .pipeline import (
        build_runtime_environment,
        build_artifact_inventory,
        build_file_audit,
        build_health_threshold_breaches,
        build_sql_model_inventory,
        DEFAULT_CONFIG_PATH,
        file_sha256,
        GOLD_CATEGORY_FIELDS,
        GOLD_CATEGORY_SQL_PATH,
        GOLD_CUSTOMER_FIELDS,
        GOLD_CUSTOMER_SQL_PATH,
        GOLD_REJECTION_FIELDS,
        GOLD_REJECTION_SQL_PATH,
        GOLD_REVENUE_FIELDS,
        GOLD_SQL_PATH,
        load_previous_run_manifest,
        load_config,
        make_staged_directory,
        raise_for_failed_reconciliation,
        raise_for_failed_metric_reconciliation,
        replace_directory_after_success,
        resolve_pipeline_path,
        write_json,
        write_text,
    )
    from .quality_checks import REQUIRED_COLUMNS, raise_for_failed_quality
except ImportError:  # Support direct execution with `python src/spark_pipeline.py`.
    from pipeline import (
        build_runtime_environment,
        build_artifact_inventory,
        build_file_audit,
        build_health_threshold_breaches,
        build_sql_model_inventory,
        DEFAULT_CONFIG_PATH,
        file_sha256,
        GOLD_CATEGORY_FIELDS,
        GOLD_CATEGORY_SQL_PATH,
        GOLD_CUSTOMER_FIELDS,
        GOLD_CUSTOMER_SQL_PATH,
        GOLD_REJECTION_FIELDS,
        GOLD_REJECTION_SQL_PATH,
        GOLD_REVENUE_FIELDS,
        GOLD_SQL_PATH,
        load_previous_run_manifest,
        load_config,
        make_staged_directory,
        raise_for_failed_reconciliation,
        raise_for_failed_metric_reconciliation,
        replace_directory_after_success,
        resolve_pipeline_path,
        write_json,
        write_text,
    )
    from quality_checks import REQUIRED_COLUMNS, raise_for_failed_quality


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
SPARK_RUN_SUMMARY_FILENAME = "spark_pipeline_run_summary.md"
SPARK_QUALITY_REPORT_FILENAME = "spark_data_quality_report.json"
LOGGER = logging.getLogger(__name__)
SPARK_OUTPUT_DESCRIPTIONS = {
    "silver_orders": "Cleaned analytics-ready orders produced by Spark.",
    "rejected_orders": "Source-valid orders excluded from Spark silver scope.",
    "gold_revenue_metrics": "Spark revenue metrics by order date and category.",
    "gold_customer_metrics": "Spark customer-level order and revenue metrics.",
    "gold_category_metrics": "Spark category-level order and revenue metrics.",
    "gold_rejection_metrics": "Spark rejected-order impact metrics.",
}


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


def _parse_order_date(value):
    if value is None:
        return None
    if hasattr(value, "date"):
        value = value.date()
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def get_latest_spark_order_date(raw_orders_df):
    latest_rows = raw_orders_df.selectExpr(
        "max(order_date) as latest_order_date"
    ).collect()
    if not latest_rows:
        return None
    latest_row = latest_rows[0]
    try:
        latest_value = latest_row["latest_order_date"]
    except (KeyError, TypeError):
        latest_value = getattr(latest_row, "latest_order_date", None)
    return _parse_order_date(latest_value)


def _spark_profile_date(value):
    parsed_date = _parse_order_date(value)
    if parsed_date is None:
        return None
    return parsed_date.isoformat()


def _optional_row_value(row, field):
    try:
        return _row_value(row, field)
    except (AttributeError, KeyError, TypeError):
        return None


def build_spark_source_profile(raw_orders_df):
    date_range_rows = raw_orders_df.selectExpr(
        "min(order_date) as min_order_date",
        "max(order_date) as max_order_date",
    ).collect()
    date_range_row = date_range_rows[0] if date_range_rows else {}
    min_order_date = _spark_profile_date(
        _optional_row_value(date_range_row, "min_order_date")
    )
    max_order_date = _spark_profile_date(
        _optional_row_value(date_range_row, "max_order_date")
    )

    high_watermark_order_id = None
    if max_order_date is not None:
        high_watermark_rows = (
            raw_orders_df.where(f"order_date = {_spark_sql_literal(max_order_date)}")
            .selectExpr("max(order_id) as order_id")
            .collect()
        )
        if high_watermark_rows:
            high_watermark_order_id = _optional_row_value(
                high_watermark_rows[0],
                "order_id",
            )

    status_counts = {}
    for row in raw_orders_df.groupBy("status").count().collect():
        status = _optional_row_value(row, "status")
        count = _optional_row_value(row, "count")
        if status is not None and count is not None:
            status_counts[status] = count

    return {
        "order_date_range": {"min": min_order_date, "max": max_order_date},
        "high_watermark": {
            "order_date": max_order_date,
            "order_id": high_watermark_order_id,
        },
        "status_counts": dict(sorted(status_counts.items())),
    }


def _spark_count_where(dataframe, predicate):
    return dataframe.where(predicate).count()


def _spark_expectation(name, success, observed):
    return {"expectation": name, "success": success, "observed": observed}


def build_spark_raw_quality_report(
    raw_orders_df,
    included_statuses=None,
    order_date_start=None,
    order_date_end=None,
):
    columns = list(raw_orders_df.columns)
    row_count = raw_orders_df.count()
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(columns))
    unexpected_columns = sorted(set(columns) - set(REQUIRED_COLUMNS))

    malformed_row_count = 0
    blank_order_id_count = 0
    duplicate_order_id_count = 0
    invalid_amount_count = 0
    invalid_date_count = 0
    blank_dimension_count = 0
    matching_status_count = 0
    selected_row_count = 0

    if not missing_columns:
        null_predicate = " or ".join(
            f"{column} is null" for column in REQUIRED_COLUMNS
        )
        malformed_row_count = _spark_count_where(raw_orders_df, null_predicate)
        blank_order_id_count = _spark_count_where(
            raw_orders_df,
            "order_id is null or trim(order_id) = ''",
        )
        duplicate_order_id_count = (
            raw_orders_df.where("order_id is not null and trim(order_id) <> ''")
            .groupBy("order_id")
            .count()
            .where("count > 1")
            .count()
        )
        invalid_amount_count = _spark_count_where(
            raw_orders_df,
            "try_cast(quantity as int) is null or try_cast(quantity as int) <= 0 "
            "or try_cast(unit_price as double) is null "
            "or try_cast(unit_price as double) <= 0",
        )
        invalid_date_count = _spark_count_where(
            raw_orders_df,
            "order_date is null or to_date(order_date, 'yyyy-MM-dd') is null "
            "or date_format(to_date(order_date, 'yyyy-MM-dd'), 'yyyy-MM-dd') "
            "<> order_date",
        )
        blank_dimension_count = _spark_count_where(
            raw_orders_df,
            " or ".join(
                f"{column} is null or trim({column}) = ''"
                for column in ["customer_id", "category", "product", "status"]
            ),
        )

        if included_statuses is not None:
            status_values = ", ".join(
                _spark_sql_literal(status) for status in included_statuses
            )
            matching_status_count = _spark_count_where(
                raw_orders_df,
                f"status in ({status_values})",
            )

            selection_predicates = [f"status in ({status_values})"]
            if order_date_start is not None:
                selection_predicates.append(
                    f"order_date >= {_spark_sql_literal(order_date_start)}"
                )
            if order_date_end is not None:
                selection_predicates.append(
                    f"order_date <= {_spark_sql_literal(order_date_end)}"
                )
            selected_row_count = _spark_count_where(
                raw_orders_df,
                " and ".join(selection_predicates),
            )

    expectations = [
        _spark_expectation(
            "dataset_is_not_empty",
            row_count > 0,
            {"row_count": row_count},
        ),
        _spark_expectation(
            "required_columns_are_present",
            not missing_columns,
            {"missing_columns": missing_columns},
        ),
        _spark_expectation(
            "raw_schema_matches_contract",
            not missing_columns and not unexpected_columns,
            {
                "required_columns": REQUIRED_COLUMNS,
                "unexpected_columns": unexpected_columns,
            },
        ),
        _spark_expectation(
            "rows_are_well_formed",
            not missing_columns and malformed_row_count == 0,
            {"malformed_row_count": malformed_row_count},
        ),
        _spark_expectation(
            "order_ids_are_populated",
            not missing_columns and blank_order_id_count == 0,
            {"invalid_row_count": blank_order_id_count},
        ),
        _spark_expectation(
            "order_id_is_unique",
            not missing_columns and duplicate_order_id_count == 0,
            {"duplicate_order_id_count": duplicate_order_id_count},
        ),
        _spark_expectation(
            "amounts_are_positive_numbers",
            not missing_columns and invalid_amount_count == 0,
            {"invalid_row_count": invalid_amount_count},
        ),
        _spark_expectation(
            "order_dates_are_iso_dates",
            not missing_columns and invalid_date_count == 0,
            {"invalid_row_count": invalid_date_count},
        ),
        _spark_expectation(
            "business_dimensions_are_populated",
            not missing_columns and blank_dimension_count == 0,
            {"invalid_row_count": blank_dimension_count},
        ),
    ]
    if included_statuses is not None:
        expectations.append(
            _spark_expectation(
                "included_statuses_match_source_rows",
                not missing_columns and matching_status_count > 0,
                {
                    "included_statuses": list(included_statuses),
                    "matching_rows": matching_status_count,
                },
            )
        )
    if included_statuses is not None and (
        order_date_start is not None or order_date_end is not None
    ):
        expectations.append(
            _spark_expectation(
                "selected_rows_match_config",
                not missing_columns and selected_row_count > 0,
                {
                    "included_statuses": list(included_statuses),
                    "order_date_start": order_date_start,
                    "order_date_end": order_date_end,
                    "matching_rows": selected_row_count,
                },
            )
        )

    failed_expectations = [
        result["expectation"] for result in expectations if not result["success"]
    ]
    return {
        "engine": "spark",
        "success": not failed_expectations,
        "row_count": row_count,
        "summary": {
            "expectations": len(expectations),
            "passed": len(expectations) - len(failed_expectations),
            "failed": len(failed_expectations),
            "failed_expectations": failed_expectations,
        },
        "expectations": expectations,
    }


def build_spark_health_warnings(
    *,
    bronze_count,
    silver_count,
    rejected_count,
    latest_order_date=None,
    warning_thresholds=None,
    as_of_date=None,
):
    warning_thresholds = warning_thresholds or {}
    warnings = []
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc).date()

    if "max_rejection_rate" in warning_thresholds:
        threshold = warning_thresholds["max_rejection_rate"]
        rejection_rate = rejected_count / bronze_count if bronze_count else 0
        if rejection_rate > threshold:
            warnings.append(
                {
                    "name": "rejection_rate_above_threshold",
                    "severity": "warning",
                    "message": (
                        "Rejected row rate exceeded configured warning threshold"
                    ),
                    "observed": {
                        "bronze_rows": bronze_count,
                        "rejected_rows": rejected_count,
                        "rejection_rate": round(rejection_rate, 6),
                    },
                    "threshold": {"max_rejection_rate": threshold},
                }
            )

    if "min_silver_rows" in warning_thresholds:
        threshold = warning_thresholds["min_silver_rows"]
        if silver_count < threshold:
            warnings.append(
                {
                    "name": "silver_rows_below_threshold",
                    "severity": "warning",
                    "message": (
                        "Silver row count fell below configured warning threshold"
                    ),
                    "observed": {"silver_rows": silver_count},
                    "threshold": {"min_silver_rows": threshold},
                }
            )

    if latest_order_date is not None and "max_source_lag_days" in warning_thresholds:
        threshold = warning_thresholds["max_source_lag_days"]
        source_lag_days = (as_of_date - latest_order_date).days
        if source_lag_days > threshold:
            warnings.append(
                {
                    "name": "source_lag_above_threshold",
                    "severity": "warning",
                    "message": (
                        "Latest source order date is older than configured "
                        "freshness threshold"
                    ),
                    "observed": {
                        "latest_order_date": latest_order_date.isoformat(),
                        "as_of_date": as_of_date.isoformat(),
                        "source_lag_days": source_lag_days,
                    },
                    "threshold": {"max_source_lag_days": threshold},
                }
            )

    if (
        latest_order_date is not None
        and "max_future_order_date_days" in warning_thresholds
    ):
        threshold = warning_thresholds["max_future_order_date_days"]
        future_order_date_days = (latest_order_date - as_of_date).days
        if future_order_date_days > threshold:
            warnings.append(
                {
                    "name": "future_order_date_above_threshold",
                    "severity": "warning",
                    "message": (
                        "Latest source order date is farther in the future "
                        "than configured warning threshold"
                    ),
                    "observed": {
                        "latest_order_date": latest_order_date.isoformat(),
                        "as_of_date": as_of_date.isoformat(),
                        "future_order_date_days": future_order_date_days,
                    },
                    "threshold": {
                        "max_future_order_date_days": threshold
                    },
                }
            )

    return warnings


def build_spark_output_contract_validation(outputs):
    validations = {}
    for artifact_name, payload in outputs.items():
        expected_columns = list(payload["expected_columns"])
        actual_columns = list(payload["dataframe"].columns)
        missing_columns = [
            column for column in expected_columns if column not in actual_columns
        ]
        unexpected_columns = [
            column for column in actual_columns if column not in expected_columns
        ]
        validations[artifact_name] = {
            "success": (
                actual_columns == expected_columns
                and not missing_columns
                and not unexpected_columns
            ),
            "expected_columns": expected_columns,
            "actual_columns": actual_columns,
            "missing_columns": missing_columns,
            "unexpected_columns": unexpected_columns,
            "order_matches": actual_columns == expected_columns,
        }

    failed_outputs = [
        artifact_name
        for artifact_name, validation in validations.items()
        if not validation["success"]
    ]
    return {
        "version": 1,
        "success": not failed_outputs,
        "failed_outputs": failed_outputs,
        "outputs": validations,
    }


def _metric_check(name, expected, actual):
    return {
        "name": name,
        "success": expected == actual,
        "expected": expected,
        "actual": actual,
        "difference": round(expected - actual, 2)
        if isinstance(expected, float) or isinstance(actual, float)
        else expected - actual,
    }


def _row_value(row, field):
    try:
        return row[field]
    except (KeyError, TypeError):
        return getattr(row, field)


def _collect_spark_metric_row(dataframe, expressions):
    rows = dataframe.selectExpr(*expressions.values()).collect()
    if not rows:
        return {}
    return {name: _row_value(rows[0], name) for name in expressions}


def build_spark_metric_reconciliation(silver_df, rejected_df, spark_outputs):
    silver_metrics = _collect_spark_metric_row(
        silver_df,
        {
            "orders": "count(*) as orders",
            "units": "cast(coalesce(sum(quantity), 0) as bigint) as units",
            "revenue": (
                "cast(coalesce(round(sum(revenue), 2), 0.0) as double) as revenue"
            ),
        },
    )
    rejected_metrics = _collect_spark_metric_row(
        rejected_df,
        {
            "orders": "count(*) as orders",
            "units": (
                "cast(coalesce(sum(cast(quantity as int)), 0) as bigint) as units"
            ),
            "revenue": (
                "cast(coalesce(round("
                "sum(cast(quantity as int) * cast(unit_price as double)), 2"
                "), 0.0) as double) as revenue"
            ),
        },
    )
    gold_revenue_metrics = _collect_spark_metric_row(
        spark_outputs["gold_revenue_metrics"]["dataframe"],
        {
            "orders": "cast(coalesce(sum(orders), 0) as bigint) as orders",
            "units": "cast(coalesce(sum(units), 0) as bigint) as units",
            "revenue": (
                "cast(coalesce(round(sum(revenue), 2), 0.0) as double) as revenue"
            ),
        },
    )
    gold_customer_metrics = _collect_spark_metric_row(
        spark_outputs["gold_customer_metrics"]["dataframe"],
        {
            "orders": "cast(coalesce(sum(orders), 0) as bigint) as orders",
            "units": "cast(coalesce(sum(units), 0) as bigint) as units",
            "revenue": (
                "cast(coalesce(round(sum(revenue), 2), 0.0) as double) as revenue"
            ),
        },
    )
    gold_category_metrics = _collect_spark_metric_row(
        spark_outputs["gold_category_metrics"]["dataframe"],
        {
            "orders": "cast(coalesce(sum(orders), 0) as bigint) as orders",
            "units": "cast(coalesce(sum(units), 0) as bigint) as units",
            "revenue": (
                "cast(coalesce(round(sum(revenue), 2), 0.0) as double) as revenue"
            ),
        },
    )
    gold_rejection_metrics = _collect_spark_metric_row(
        spark_outputs["gold_rejection_metrics"]["dataframe"],
        {
            "orders": (
                "cast(coalesce(sum(rejected_orders), 0) as bigint) as orders"
            ),
            "units": (
                "cast(coalesce(sum(rejected_units), 0) as bigint) as units"
            ),
            "revenue": (
                "cast(coalesce(round(sum(potential_revenue), 2), 0.0) as double) "
                "as revenue"
            ),
        },
    )

    checks = [
        _metric_check(
            "gold_revenue_orders_match_silver",
            silver_metrics["orders"],
            gold_revenue_metrics["orders"],
        ),
        _metric_check(
            "gold_revenue_units_match_silver",
            silver_metrics["units"],
            gold_revenue_metrics["units"],
        ),
        _metric_check(
            "gold_revenue_amount_match_silver",
            silver_metrics["revenue"],
            gold_revenue_metrics["revenue"],
        ),
        _metric_check(
            "gold_customer_orders_match_silver",
            silver_metrics["orders"],
            gold_customer_metrics["orders"],
        ),
        _metric_check(
            "gold_customer_units_match_silver",
            silver_metrics["units"],
            gold_customer_metrics["units"],
        ),
        _metric_check(
            "gold_customer_revenue_match_silver",
            silver_metrics["revenue"],
            gold_customer_metrics["revenue"],
        ),
        _metric_check(
            "gold_category_orders_match_silver",
            silver_metrics["orders"],
            gold_category_metrics["orders"],
        ),
        _metric_check(
            "gold_category_units_match_silver",
            silver_metrics["units"],
            gold_category_metrics["units"],
        ),
        _metric_check(
            "gold_category_revenue_match_silver",
            silver_metrics["revenue"],
            gold_category_metrics["revenue"],
        ),
        _metric_check(
            "gold_rejection_orders_match_rejected",
            rejected_metrics["orders"],
            gold_rejection_metrics["orders"],
        ),
        _metric_check(
            "gold_rejection_units_match_rejected",
            rejected_metrics["units"],
            gold_rejection_metrics["units"],
        ),
        _metric_check(
            "gold_rejection_revenue_match_rejected",
            rejected_metrics["revenue"],
            gold_rejection_metrics["revenue"],
        ),
    ]
    failed_checks = [check["name"] for check in checks if not check["success"]]
    return {
        "version": 1,
        "success": not failed_checks,
        "failed_checks": failed_checks,
        "checks": checks,
    }


def _read_spark_sql_model(sql_path):
    return Path(sql_path).read_text(encoding="utf-8").strip().rstrip(";")


def build_spark_gold_dataframes(spark, silver_df, rejected_df):
    silver_df.createOrReplaceTempView("silver_orders")
    rejected_df.createOrReplaceTempView("rejected_orders")
    return {
        "gold_revenue_metrics": {
            "dataframe": spark.sql(_read_spark_sql_model(GOLD_SQL_PATH)),
            "expected_columns": GOLD_REVENUE_FIELDS,
            "path_name": "spark_gold_revenue_metrics",
        },
        "gold_customer_metrics": {
            "dataframe": spark.sql(_read_spark_sql_model(GOLD_CUSTOMER_SQL_PATH)),
            "expected_columns": GOLD_CUSTOMER_FIELDS,
            "path_name": "spark_gold_customer_metrics",
        },
        "gold_category_metrics": {
            "dataframe": spark.sql(_read_spark_sql_model(GOLD_CATEGORY_SQL_PATH)),
            "expected_columns": GOLD_CATEGORY_FIELDS,
            "path_name": "spark_gold_category_metrics",
        },
        "gold_rejection_metrics": {
            "dataframe": spark.sql(_read_spark_sql_model(GOLD_REJECTION_SQL_PATH)),
            "expected_columns": GOLD_REJECTION_FIELDS,
            "path_name": "spark_gold_rejection_metrics",
        },
    }


def raise_for_failed_spark_output_contract_validation(validation):
    if not validation["success"]:
        raise ValueError(
            "Spark output contract validation failed: "
            f"{', '.join(validation['failed_outputs'])}"
        )


def write_spark_parquet_outputs(outputs):
    staged_outputs = {}
    try:
        for artifact_name, payload in outputs.items():
            target_path = Path(payload["path"])
            staged_path = make_staged_directory(target_path)
            staged_outputs[artifact_name] = {
                "target_path": target_path,
                "staged_path": staged_path,
            }
            payload["dataframe"].write.mode("overwrite").parquet(str(staged_path))

        for payload in staged_outputs.values():
            replace_directory_after_success(
                payload["target_path"],
                payload["staged_path"],
            )
    except Exception:
        for payload in staged_outputs.values():
            shutil.rmtree(payload["staged_path"], ignore_errors=True)
        raise


def build_spark_data_catalog(
    output_paths,
    output_row_counts,
    output_contract_validation,
):
    return {
        "version": 1,
        "format": "manifest_embedded",
        "engine": "spark",
        "tables": [
            {
                "name": output_name,
                "description": SPARK_OUTPUT_DESCRIPTIONS.get(
                    output_name,
                    "Spark published Parquet table.",
                ),
                "format": "parquet",
                "path": str(output_paths[output_name]),
                "rows": output_row_counts.get(output_name),
                "columns": [
                    {"name": column_name}
                    for column_name in output_contract_validation["outputs"][
                        output_name
                    ]["expected_columns"]
                ],
            }
            for output_name in sorted(output_paths)
        ],
    }


def build_spark_lineage(*, raw_path, processed_dir, output_paths):
    nodes = [
        {
            "id": "source.raw_orders",
            "type": "source",
            "path": str(raw_path),
        },
        {
            "id": "quality.spark_raw_order_expectations",
            "type": "quality_report",
            "path": str(processed_dir / SPARK_QUALITY_REPORT_FILENAME),
        },
        {
            "id": "catalog.spark_data_catalog",
            "type": "metadata",
            "embedded_in": str(processed_dir / SPARK_MANIFEST_FILENAME),
        },
    ]
    for output_name in sorted(output_paths):
        node = {
            "id": f"spark.{output_name}",
            "type": "table",
            "engine": "spark",
            "format": "parquet",
            "path": str(output_paths[output_name]),
        }
        if output_name.startswith("gold_"):
            node["layer"] = "gold"
            node["type"] = "sql_model"
        elif output_name == "rejected_orders":
            node["layer"] = "silver_audit"
        else:
            node["layer"] = "silver"
        nodes.append(node)

    candidate_edges = [
        ("source.raw_orders", "quality.spark_raw_order_expectations"),
        ("source.raw_orders", "spark.silver_orders"),
        ("source.raw_orders", "spark.rejected_orders"),
        ("spark.silver_orders", "spark.gold_revenue_metrics"),
        ("spark.silver_orders", "spark.gold_customer_metrics"),
        ("spark.silver_orders", "spark.gold_category_metrics"),
        ("spark.rejected_orders", "spark.gold_rejection_metrics"),
    ]
    node_ids = {node["id"] for node in nodes}
    edges = [
        {"from": source, "to": target}
        for source, target in candidate_edges
        if source in node_ids and target in node_ids
    ]
    for output_name in sorted(output_paths):
        edges.append(
            {"from": f"spark.{output_name}", "to": "catalog.spark_data_catalog"}
        )

    return {
        "version": 1,
        "engine": "spark",
        "root": str(processed_dir),
        "nodes": nodes,
        "edges": edges,
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
    output_contract_validation,
    metric_reconciliation,
    output_inventory,
    health_warnings,
    output_row_counts,
    output_paths,
    source_profile,
    quality_report,
):
    output_paths = dict(output_paths)
    return {
        "version": 1,
        "engine": "spark",
        "run": {
            "config_path": str(Path(config_path).resolve()),
            "config_sha256": file_sha256(config_path),
            "config_file_audit": build_file_audit(Path(config_path).resolve()),
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
            "file_audit": build_file_audit(raw_path),
            "rows": reconciliation["bronze_rows"],
            "profile": source_profile,
        },
        "quality": {
            "success": quality_report["success"],
            "summary": quality_report["summary"],
            "expectations": quality_report["expectations"],
        },
        "config": {
            "included_statuses": list(config["included_statuses"]),
            "order_date_window": {
                "start": config.get("order_date_start"),
                "end": config.get("order_date_end"),
            },
            "warning_thresholds": dict(config.get("warning_thresholds", {})),
        },
        "outputs": {
            output_name: {
                "path": str(output_path),
                "format": "parquet",
                "columns": list(
                    output_contract_validation["outputs"][output_name][
                        "expected_columns"
                    ]
                ),
                "rows": output_row_counts[output_name],
            }
            for output_name, output_path in output_paths.items()
        },
        "data_catalog": build_spark_data_catalog(
            output_paths,
            output_row_counts,
            output_contract_validation,
        ),
        "lineage": build_spark_lineage(
            raw_path=raw_path,
            processed_dir=processed_dir,
            output_paths=output_paths,
        ),
        "output_inventory": output_inventory,
        "sql_models": build_sql_model_inventory(output_paths),
        "health": {
            "status": "warning" if health_warnings else "passed",
            "warnings": health_warnings,
            "warning_count": len(health_warnings),
            "threshold_breaches": build_health_threshold_breaches(
                health_warnings
            ),
        },
        "reconciliation": reconciliation,
        "metric_reconciliation": metric_reconciliation,
        "schema_contract_validation": output_contract_validation,
    }


def _spark_outputs(manifest):
    outputs = manifest.get("outputs", {})
    if not isinstance(outputs, dict):
        return {}
    return outputs


def _spark_output_rows(manifest, output_name):
    output = _spark_outputs(manifest).get(output_name, {})
    if not isinstance(output, dict):
        return None
    return output.get("rows")


def _delta(previous_value, current_value):
    if previous_value is None or current_value is None:
        return None
    return current_value - previous_value


def _spark_manifest_config(manifest):
    config = manifest.get("config", {})
    if not isinstance(config, dict):
        return {}
    return config


def _spark_included_statuses(manifest):
    statuses = _spark_manifest_config(manifest).get("included_statuses")
    if not isinstance(statuses, list):
        return []
    return list(statuses)


def _spark_order_date_window(manifest):
    window = _spark_manifest_config(manifest).get("order_date_window")
    if not isinstance(window, dict):
        return {"start": None, "end": None}
    return {
        "start": window.get("start"),
        "end": window.get("end"),
    }


def _spark_warning_thresholds(manifest):
    thresholds = _spark_manifest_config(manifest).get("warning_thresholds")
    if not isinstance(thresholds, dict):
        return {}
    return dict(thresholds)


def _spark_output_inventory(manifest):
    inventory = manifest.get("output_inventory", {})
    if not isinstance(inventory, dict):
        return {}
    return inventory


def _spark_health(manifest):
    health = manifest.get("health", {})
    if not isinstance(health, dict):
        return {}
    return health


def _spark_health_status(manifest):
    return _spark_health(manifest).get("status")


def _spark_warning_count(manifest):
    return _spark_health(manifest).get("warning_count")


def _spark_source_profile(manifest):
    profile = manifest.get("source", {}).get("profile", {})
    if not isinstance(profile, dict):
        return {}
    return profile


def _spark_source_status_counts(manifest):
    counts = _spark_source_profile(manifest).get("status_counts")
    if not isinstance(counts, dict):
        return {}
    return counts


def _spark_source_high_watermark(manifest):
    watermark = _spark_source_profile(manifest).get("high_watermark")
    if not isinstance(watermark, dict):
        return None
    return {
        "order_date": watermark.get("order_date"),
        "order_id": watermark.get("order_id"),
    }


def build_spark_run_comparison(
    current_manifest,
    previous_manifest=None,
    unavailable_reason=None,
):
    if previous_manifest is None:
        return {
            "version": 1,
            "previous_manifest_available": False,
            "unavailable_reason": unavailable_reason or "not_found",
        }

    output_names = sorted(
        set(_spark_outputs(previous_manifest)) | set(_spark_outputs(current_manifest))
    )
    output_row_deltas = {}
    for output_name in output_names:
        previous_rows = _spark_output_rows(previous_manifest, output_name)
        current_rows = _spark_output_rows(current_manifest, output_name)
        output_row_deltas[output_name] = {
            "previous": previous_rows,
            "current": current_rows,
            "delta": _delta(previous_rows, current_rows),
        }

    previous_statuses = _spark_included_statuses(previous_manifest)
    current_statuses = _spark_included_statuses(current_manifest)
    previous_window = _spark_order_date_window(previous_manifest)
    current_window = _spark_order_date_window(current_manifest)
    previous_thresholds = _spark_warning_thresholds(previous_manifest)
    current_thresholds = _spark_warning_thresholds(current_manifest)
    threshold_changes = {}
    for threshold_name in sorted(set(previous_thresholds) | set(current_thresholds)):
        previous_value = previous_thresholds.get(threshold_name)
        current_value = current_thresholds.get(threshold_name)
        threshold_changes[threshold_name] = {
            "previous": previous_value,
            "current": current_value,
            "changed": previous_value != current_value,
        }

    previous_inventory = _spark_output_inventory(previous_manifest)
    current_inventory = _spark_output_inventory(current_manifest)
    output_checksum_changes = {}
    for output_name in sorted(set(previous_inventory) | set(current_inventory)):
        previous_stats = previous_inventory.get(output_name, {})
        current_stats = current_inventory.get(output_name, {})
        if not isinstance(previous_stats, dict):
            previous_stats = {}
        if not isinstance(current_stats, dict):
            current_stats = {}
        previous_sha = previous_stats.get("sha256")
        current_sha = current_stats.get("sha256")
        output_checksum_changes[output_name] = {
            "previous_sha256": previous_sha,
            "current_sha256": current_sha,
            "sha256_changed": previous_sha != current_sha,
        }

    previous_status_counts = _spark_source_status_counts(previous_manifest)
    current_status_counts = _spark_source_status_counts(current_manifest)
    source_status_count_deltas = {}
    for status in sorted(set(previous_status_counts) | set(current_status_counts)):
        previous_count = previous_status_counts.get(status)
        current_count = current_status_counts.get(status)
        source_status_count_deltas[status] = {
            "previous": previous_count,
            "current": current_count,
            "delta": _delta(previous_count, current_count),
        }

    previous_high_watermark = _spark_source_high_watermark(previous_manifest)
    current_high_watermark = _spark_source_high_watermark(current_manifest)

    return {
        "version": 1,
        "previous_manifest_available": True,
        "previous_completed_at_utc": previous_manifest.get("run", {}).get(
            "completed_at_utc"
        ),
        "current_completed_at_utc": current_manifest.get("run", {}).get(
            "completed_at_utc"
        ),
        "source_sha256_changed": (
            previous_manifest.get("source", {}).get("sha256")
            != current_manifest.get("source", {}).get("sha256")
        ),
        "config_sha256_changed": (
            previous_manifest.get("run", {}).get("config_sha256")
            != current_manifest.get("run", {}).get("config_sha256")
        ),
        "config_scope_changes": {
            "included_statuses": {
                "previous": previous_statuses,
                "current": current_statuses,
                "added": sorted(set(current_statuses) - set(previous_statuses)),
                "removed": sorted(set(previous_statuses) - set(current_statuses)),
                "changed": previous_statuses != current_statuses,
            },
            "order_date_window": {
                "previous": previous_window,
                "current": current_window,
                "changed": previous_window != current_window,
            },
            "warning_thresholds": threshold_changes,
        },
        "health_status_changed": (
            _spark_health_status(previous_manifest)
            != _spark_health_status(current_manifest)
        ),
        "warning_count_delta": _delta(
            _spark_warning_count(previous_manifest),
            _spark_warning_count(current_manifest),
        ),
        "source_high_watermark": {
            "previous": previous_high_watermark,
            "current": current_high_watermark,
            "changed": previous_high_watermark != current_high_watermark,
        },
        "source_status_count_deltas": source_status_count_deltas,
        "output_row_deltas": output_row_deltas,
        "output_checksum_changes": output_checksum_changes,
    }


def _format_spark_summary_value(value):
    if value is None:
        return "n/a"
    return str(value)


def _format_spark_delta(delta):
    if delta is None:
        return "n/a"
    if isinstance(delta, (int, float)) and delta > 0:
        return f"+{delta}"
    return str(delta)


def _format_spark_watermark(watermark):
    if not isinstance(watermark, dict):
        return "n/a"
    order_date = watermark.get("order_date")
    order_id = watermark.get("order_id")
    if order_date is None and order_id is None:
        return "n/a"
    return (
        f"{_format_spark_summary_value(order_date)} "
        f"(`{_format_spark_summary_value(order_id)}`)"
    )


def build_spark_run_summary_markdown(manifest):
    run = manifest.get("run", {})
    source = manifest.get("source", {})
    source_profile = source.get("profile", {})
    if not isinstance(source_profile, dict):
        source_profile = {}
    config = manifest.get("config", {})
    if not isinstance(config, dict):
        config = {}
    health = manifest.get("health", {})
    if not isinstance(health, dict):
        health = {}
    reconciliation = manifest.get("reconciliation", {})
    if not isinstance(reconciliation, dict):
        reconciliation = {}
    metric_reconciliation = manifest.get("metric_reconciliation", {})
    if not isinstance(metric_reconciliation, dict):
        metric_reconciliation = {}
    outputs = _spark_outputs(manifest)
    comparison = manifest.get("run_comparison", {})
    if not isinstance(comparison, dict):
        comparison = {}

    order_date_window = config.get("order_date_window", {})
    if not isinstance(order_date_window, dict):
        order_date_window = {}

    lines = [
        "# Spark Pipeline Run Summary",
        "",
        f"- Completed: {_format_spark_summary_value(run.get('completed_at_utc'))}",
        f"- Source: `{_format_spark_summary_value(source.get('path'))}`",
        f"- Health: {_format_spark_summary_value(health.get('status'))}",
        f"- Quality: {'passed' if manifest.get('quality', {}).get('success') else 'failed'}",
        f"- Health warnings: {health.get('warning_count', 0)}",
        f"- Reconciliation: "
        f"{'passed' if reconciliation.get('success') else 'failed'}",
        f"- Metric reconciliation: "
        f"{'passed' if metric_reconciliation.get('success') else 'failed'}",
        f"- Previous manifest: "
        f"{_format_spark_summary_value(comparison.get('previous_manifest_available'))}",
        "",
        "## Config",
        "",
        f"- Included statuses: "
        f"{', '.join(config.get('included_statuses', [])) or 'n/a'}",
        f"- Order date window: "
        f"{_format_spark_summary_value(order_date_window.get('start'))} "
        f"to {_format_spark_summary_value(order_date_window.get('end'))}",
        "",
        "## Source Profile",
        "",
        f"- Rows: {_format_spark_summary_value(source.get('rows'))}",
        f"- High watermark: "
        f"{_format_spark_watermark(source_profile.get('high_watermark'))}",
        "",
        "| Status | Rows | Delta |",
        "| --- | ---: | ---: |",
    ]

    status_counts = source_profile.get("status_counts", {})
    if not isinstance(status_counts, dict):
        status_counts = {}
    status_deltas = comparison.get("source_status_count_deltas", {})
    if not isinstance(status_deltas, dict):
        status_deltas = {}
    for status, rows in sorted(status_counts.items()):
        delta = status_deltas.get(status, {})
        if not isinstance(delta, dict):
            delta = {}
        lines.append(
            f"| `{status}` | {_format_spark_summary_value(rows)} | "
            f"{_format_spark_delta(delta.get('delta'))} |"
        )
    if not status_counts:
        lines.append("| n/a | n/a | n/a |")

    quality = manifest.get("quality", {})
    if isinstance(quality, dict):
        expectations = quality.get("expectations", [])
        if isinstance(expectations, list) and expectations:
            lines.extend(
                [
                    "",
                    "## Quality Expectations",
                    "",
                    "| Expectation | Status | Observed |",
                    "| --- | --- | --- |",
                ]
            )
            for expectation in expectations:
                if not isinstance(expectation, dict):
                    continue
                status = "passed" if expectation.get("success") else "failed"
                lines.append(
                    f"| `{_format_spark_summary_value(expectation.get('expectation'))}` | "
                    f"{status} | "
                    f"`{_format_spark_summary_value(expectation.get('observed'))}` |"
                )

    lines.extend(
        [
            "",
            "## Published Outputs",
            "",
            "| Output | Rows | Delta | Checksum changed |",
            "| --- | ---: | ---: | --- |",
        ]
    )
    row_deltas = comparison.get("output_row_deltas", {})
    if not isinstance(row_deltas, dict):
        row_deltas = {}
    checksum_changes = comparison.get("output_checksum_changes", {})
    if not isinstance(checksum_changes, dict):
        checksum_changes = {}
    for output_name, output in sorted(outputs.items()):
        if not isinstance(output, dict):
            continue
        row_delta = row_deltas.get(output_name, {})
        if not isinstance(row_delta, dict):
            row_delta = {}
        checksum_change = checksum_changes.get(output_name, {})
        if not isinstance(checksum_change, dict):
            checksum_change = {}
        lines.append(
            f"| `{output_name}` | {_format_spark_summary_value(output.get('rows'))} | "
            f"{_format_spark_delta(row_delta.get('delta'))} | "
            f"{_format_spark_summary_value(checksum_change.get('sha256_changed'))} |"
        )

    warnings = health.get("warnings", [])
    if isinstance(warnings, list) and warnings:
        lines.extend(["", "## Health Warnings", ""])
        for warning in warnings:
            if not isinstance(warning, dict):
                continue
            lines.append(
                f"- `{_format_spark_summary_value(warning.get('name'))}`: "
                f"{_format_spark_summary_value(warning.get('message'))}"
            )

    threshold_breaches = health.get("threshold_breaches", [])
    if isinstance(threshold_breaches, list) and threshold_breaches:
        lines.extend(
            [
                "",
                "## Threshold Breaches",
                "",
                "| Name | Observed | Threshold |",
                "| --- | --- | --- |",
            ]
        )
        for breach in threshold_breaches:
            if not isinstance(breach, dict):
                continue
            lines.append(
                f"| `{_format_spark_summary_value(breach.get('name'))}` | "
                f"`{_format_spark_summary_value(breach.get('observed'))}` | "
                f"`{_format_spark_summary_value(breach.get('threshold'))}` |"
            )

    return "\n".join(lines) + "\n"


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
        quality_report = build_spark_raw_quality_report(
            raw_orders_df,
            included_statuses=config["included_statuses"],
            order_date_start=config.get("order_date_start"),
            order_date_end=config.get("order_date_end"),
        )
        quality_report_path = processed_dir / SPARK_QUALITY_REPORT_FILENAME
        write_json(quality_report_path, quality_report)
        LOGGER.info("Wrote Spark data quality report to %s", quality_report_path)
        if not quality_report["success"]:
            summary_path = processed_dir / SPARK_RUN_SUMMARY_FILENAME
            write_text(
                summary_path,
                build_spark_run_summary_markdown(
                    {
                        "run": {
                            "completed_at_utc": None,
                            "config_path": str(config_path.resolve()),
                        },
                        "source": {
                            "path": str(raw_path),
                            "rows": quality_report["row_count"],
                        },
                        "quality": {
                            "success": quality_report["success"],
                            "summary": quality_report["summary"],
                            "expectations": quality_report["expectations"],
                        },
                        "config": {
                            "included_statuses": config["included_statuses"],
                            "order_date_window": {
                                "start": config.get("order_date_start"),
                                "end": config.get("order_date_end"),
                            },
                        },
                    }
                ),
            )
            LOGGER.info("Wrote failed Spark quality run summary to %s", summary_path)
        raise_for_failed_quality(quality_report)
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
        source_profile = build_spark_source_profile(raw_orders_df)
        latest_order_date = None
        if (
            "max_source_lag_days" in config["warning_thresholds"]
            or "max_future_order_date_days" in config["warning_thresholds"]
        ):
            latest_order_date = get_latest_spark_order_date(raw_orders_df)
        health_warnings = build_spark_health_warnings(
            bronze_count=reconciliation["bronze_rows"],
            silver_count=reconciliation["silver_rows"],
            rejected_count=reconciliation["rejected_rows"],
            latest_order_date=latest_order_date,
            warning_thresholds=config["warning_thresholds"],
            as_of_date=started_at.date(),
        )
        for warning in health_warnings:
            LOGGER.warning("%s: %s", warning["name"], warning["message"])
        spark_outputs = {
            "silver_orders": {
                "dataframe": silver_df,
                "expected_columns": SILVER_COLUMNS,
                "path_name": "spark_silver_orders",
            },
            "rejected_orders": {
                "dataframe": rejected_df,
                "expected_columns": REJECTED_COLUMNS,
                "path_name": "spark_rejected_orders",
            },
            **build_spark_gold_dataframes(spark, silver_df, rejected_df),
        }
        output_contract_validation = build_spark_output_contract_validation(
            spark_outputs
        )
        raise_for_failed_spark_output_contract_validation(
            output_contract_validation
        )
        metric_reconciliation = build_spark_metric_reconciliation(
            silver_df,
            rejected_df,
            spark_outputs,
        )
        raise_for_failed_metric_reconciliation(metric_reconciliation)
        output_paths = {
            output_name: processed_dir / payload["path_name"]
            for output_name, payload in spark_outputs.items()
        }
        output_row_counts = {
            "silver_orders": reconciliation["silver_rows"],
            "rejected_orders": reconciliation["rejected_rows"],
        }
        output_row_counts.update(
            {
                output_name: payload["dataframe"].count()
                for output_name, payload in spark_outputs.items()
                if output_name not in output_row_counts
            }
        )
        write_spark_parquet_outputs(
            {
                output_name: {
                    "dataframe": payload["dataframe"],
                    "path": output_paths[output_name],
                }
                for output_name, payload in spark_outputs.items()
            }
        )
        output_inventory = build_artifact_inventory(output_paths)
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
            output_contract_validation=output_contract_validation,
            metric_reconciliation=metric_reconciliation,
            output_inventory=output_inventory,
            health_warnings=health_warnings,
            output_row_counts=output_row_counts,
            output_paths=output_paths,
            source_profile=source_profile,
            quality_report=quality_report,
        )
        manifest_path = processed_dir / SPARK_MANIFEST_FILENAME
        previous_manifest, previous_manifest_unavailable_reason = (
            load_previous_run_manifest(manifest_path)
        )
        manifest["run_comparison"] = build_spark_run_comparison(
            manifest,
            previous_manifest=previous_manifest,
            unavailable_reason=previous_manifest_unavailable_reason,
        )
        write_json(manifest_path, manifest)
        summary_path = processed_dir / SPARK_RUN_SUMMARY_FILENAME
        write_text(summary_path, build_spark_run_summary_markdown(manifest))
        return {
            "silver_path": str(output_paths["silver_orders"]),
            "rejected_path": str(output_paths["rejected_orders"]),
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the Spark silver-layer retail lakehouse adapter."
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        type=Path,
        help=(
            "Path to pipeline config JSON. Defaults to "
            f"{DEFAULT_CONFIG_PATH}."
        ),
    )
    return parser.parse_args(argv)


def cli(argv=None):
    args = parse_args(argv)
    return run_spark_silver_pipeline(args.config)


if __name__ == "__main__":
    cli()
