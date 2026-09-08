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
        replace_directory_after_success,
        resolve_pipeline_path,
        write_json,
    )
    from .quality_checks import REQUIRED_COLUMNS
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
        replace_directory_after_success,
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
    output_inventory,
    health_warnings,
    output_row_counts,
    output_paths,
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
        "output_row_deltas": output_row_deltas,
        "output_checksum_changes": output_checksum_changes,
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
            output_inventory=output_inventory,
            health_warnings=health_warnings,
            output_row_counts=output_row_counts,
            output_paths=output_paths,
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
