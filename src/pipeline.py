import argparse
import csv
from collections import Counter
import hashlib
import json
import logging
import math
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from .quality_checks import evaluate_quality, raise_for_failed_quality
    from .sql_transforms import (
        run_gold_model,
        run_gold_revenue_model,
        run_rejected_order_model,
    )
except ImportError:  # Support direct execution with `python src/pipeline.py`.
    from quality_checks import evaluate_quality, raise_for_failed_quality
    from sql_transforms import (
        run_gold_model,
        run_gold_revenue_model,
        run_rejected_order_model,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config" / "pipeline.json"
GOLD_SQL_PATH = ROOT / "sql" / "gold_revenue_metrics.sql"
GOLD_CUSTOMER_SQL_PATH = ROOT / "sql" / "gold_customer_metrics.sql"
GOLD_CATEGORY_SQL_PATH = ROOT / "sql" / "gold_category_metrics.sql"
GOLD_REJECTION_SQL_PATH = ROOT / "sql" / "gold_rejection_metrics.sql"
GOLD_REVENUE_FIELDS = [
    "order_date", "category", "orders", "units", "revenue",
    "average_order_value",
]
GOLD_CUSTOMER_FIELDS = [
    "customer_id", "orders", "units", "revenue", "first_order_date",
    "last_order_date",
]
GOLD_CATEGORY_FIELDS = [
    "category", "orders", "customers", "units", "revenue",
    "average_order_value", "first_order_date", "last_order_date",
]
GOLD_REJECTION_FIELDS = [
    "rejection_reason", "status", "order_date", "category",
    "rejected_orders", "rejected_units", "potential_revenue",
]
BRONZE_FIELD_TYPES = {
    "order_id": "string",
    "customer_id": "string",
    "order_date": "date",
    "category": "string",
    "product": "string",
    "quantity": "string",
    "unit_price": "string",
    "status": "string",
}
REJECTED_FIELD_TYPES = {
    **BRONZE_FIELD_TYPES,
    "rejection_reason": "string",
}
SILVER_FIELD_TYPES = {
    "order_id": "string",
    "customer_id": "string",
    "order_date": "date",
    "category": "string",
    "product": "string",
    "quantity": "integer",
    "unit_price": "float",
    "revenue": "float",
}
GOLD_FIELD_TYPES = {
    "order_date": "date",
    "category": "string",
    "orders": "integer",
    "units": "integer",
    "revenue": "float",
    "average_order_value": "float",
}
GOLD_CUSTOMER_FIELD_TYPES = {
    "customer_id": "string",
    "orders": "integer",
    "units": "integer",
    "revenue": "float",
    "first_order_date": "date",
    "last_order_date": "date",
}
GOLD_CATEGORY_FIELD_TYPES = {
    "category": "string",
    "orders": "integer",
    "customers": "integer",
    "units": "integer",
    "revenue": "float",
    "average_order_value": "float",
    "first_order_date": "date",
    "last_order_date": "date",
}
GOLD_REJECTION_FIELD_TYPES = {
    "rejection_reason": "string",
    "status": "string",
    "order_date": "date",
    "category": "string",
    "rejected_orders": "integer",
    "rejected_units": "integer",
    "potential_revenue": "float",
}
SQL_MODEL_DEFINITIONS = [
    {
        "name": "gold_revenue_metrics",
        "path": GOLD_SQL_PATH,
        "input_tables": ["silver_orders"],
        "output_artifact": "gold_revenue_metrics",
        "output_columns": GOLD_REVENUE_FIELDS,
    },
    {
        "name": "gold_customer_metrics",
        "path": GOLD_CUSTOMER_SQL_PATH,
        "input_tables": ["silver_orders"],
        "output_artifact": "gold_customer_metrics",
        "output_columns": GOLD_CUSTOMER_FIELDS,
    },
    {
        "name": "gold_category_metrics",
        "path": GOLD_CATEGORY_SQL_PATH,
        "input_tables": ["silver_orders"],
        "output_artifact": "gold_category_metrics",
        "output_columns": GOLD_CATEGORY_FIELDS,
    },
    {
        "name": "gold_rejection_metrics",
        "path": GOLD_REJECTION_SQL_PATH,
        "input_tables": ["rejected_orders"],
        "output_artifact": "gold_rejection_metrics",
        "output_columns": GOLD_REJECTION_FIELDS,
    },
]
INGESTION_HISTORY_FILENAME = "ingestion_history.json"
LOGGER = logging.getLogger(__name__)
SUPPORTED_WARNING_THRESHOLDS = {
    "max_rejection_rate",
    "max_source_lag_days",
    "min_silver_rows",
}


def load_config(path=DEFAULT_CONFIG_PATH):
    with Path(path).open(encoding="utf-8") as file:
        config = json.load(file)

    required = {"raw_path", "processed_dir", "included_statuses"}
    missing = required - set(config)
    if missing:
        raise ValueError(f"Missing configuration keys: {sorted(missing)}")

    path_keys = ["raw_path", "processed_dir"]
    invalid_path_keys = [
        key
        for key in path_keys
        if not isinstance(config[key], str) or not config[key].strip()
    ]
    if invalid_path_keys:
        raise ValueError(
            "Configuration keys must be non-empty strings: "
            f"{invalid_path_keys}"
        )

    included_statuses = config["included_statuses"]
    if (
        not isinstance(included_statuses, list)
        or not included_statuses
        or any(
            not isinstance(status, str) or not status.strip()
            for status in included_statuses
        )
    ):
        raise ValueError(
            "Configuration key 'included_statuses' must be a non-empty list "
            "of non-empty strings"
        )

    duplicate_statuses = sorted(
        status
        for status, count in Counter(included_statuses).items()
        if count > 1
    )
    if duplicate_statuses:
        raise ValueError(
            "Configuration key 'included_statuses' contains duplicate "
            f"values: {duplicate_statuses}"
        )

    for key in ["order_date_start", "order_date_end"]:
        if key not in config or config[key] is None:
            continue
        if not isinstance(config[key], str) or not _is_iso_date(config[key]):
            raise ValueError(
                f"Configuration key '{key}' must be a YYYY-MM-DD date string"
            )

    if (
        config.get("order_date_start") is not None
        and config.get("order_date_end") is not None
        and config["order_date_start"] > config["order_date_end"]
    ):
        raise ValueError(
            "Configuration key 'order_date_start' must be on or before "
            "'order_date_end'"
        )

    warning_thresholds = config.get("warning_thresholds", {})
    if warning_thresholds is None:
        warning_thresholds = {}
    if not isinstance(warning_thresholds, dict):
        raise ValueError("Configuration key 'warning_thresholds' must be an object")

    unsupported_thresholds = sorted(
        set(warning_thresholds) - SUPPORTED_WARNING_THRESHOLDS
    )
    if unsupported_thresholds:
        raise ValueError(
            "Configuration key 'warning_thresholds' contains unsupported "
            f"keys: {unsupported_thresholds}"
        )

    if "max_rejection_rate" in warning_thresholds:
        max_rejection_rate = warning_thresholds["max_rejection_rate"]
        if (
            isinstance(max_rejection_rate, bool)
            or not isinstance(max_rejection_rate, (int, float))
            or max_rejection_rate < 0
            or max_rejection_rate > 1
        ):
            raise ValueError(
                "Configuration key 'warning_thresholds.max_rejection_rate' "
                "must be a number between 0 and 1"
            )

    if "min_silver_rows" in warning_thresholds:
        min_silver_rows = warning_thresholds["min_silver_rows"]
        if (
            isinstance(min_silver_rows, bool)
            or not isinstance(min_silver_rows, int)
            or min_silver_rows < 0
        ):
            raise ValueError(
                "Configuration key 'warning_thresholds.min_silver_rows' "
                "must be a non-negative integer"
            )

    if "max_source_lag_days" in warning_thresholds:
        max_source_lag_days = warning_thresholds["max_source_lag_days"]
        if (
            isinstance(max_source_lag_days, bool)
            or not isinstance(max_source_lag_days, int)
            or max_source_lag_days < 0
        ):
            raise ValueError(
                "Configuration key 'warning_thresholds.max_source_lag_days' "
                "must be a non-negative integer"
            )

    config["warning_thresholds"] = warning_thresholds
    return config


def _is_iso_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat() == value
    except (TypeError, ValueError):
        return False


def _is_within_order_date_window(row, start=None, end=None):
    order_date = row["order_date"]
    if start is not None and order_date < start:
        return False
    if end is not None and order_date > end:
        return False
    return True


def resolve_pipeline_path(path_value, base_dir=ROOT):
    path = Path(path_value)
    if path.is_absolute():
        return path
    return base_dir / path


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            newline="",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:
            temp_path = Path(file.name)
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temp_path, path)
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:
            temp_path = Path(file.name)
            json.dump(value, file, indent=2)
            file.write("\n")
        os.replace(temp_path, path)
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise


def write_text(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:
            temp_path = Path(file.name)
            file.write(value)
        os.replace(temp_path, path)
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_sha256(path):
    path = Path(path)
    if path.is_file():
        return file_sha256(path)
    if not path.is_dir():
        return None

    digest = hashlib.sha256()
    for file_path in _artifact_file_paths(path):
        relative_path = file_path.relative_to(path).as_posix()
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        with file_path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def load_ingestion_history(path):
    path = Path(path)
    if not path.exists():
        return {"version": 1, "sources": []}

    with path.open(encoding="utf-8") as file:
        history = json.load(file)

    if history.get("version") != 1 or not isinstance(history.get("sources"), list):
        raise ValueError(f"Unsupported ingestion history format: {path}")
    return history


def update_ingestion_history(history, *, source_path, source_sha256, rows, seen_at_utc):
    sources = history.setdefault("sources", [])
    source_path = str(source_path)
    matching_source = None
    for source in sources:
        if source.get("sha256") == source_sha256:
            matching_source = source
            break

    if matching_source is None:
        matching_source = {
            "sha256": source_sha256,
            "first_seen_at_utc": seen_at_utc,
            "last_seen_at_utc": seen_at_utc,
            "run_count": 0,
            "rows": rows,
            "paths": [],
        }
        sources.append(matching_source)
        classification = "new_source_file"
    elif source_path in matching_source.get("paths", []):
        classification = "repeated_source_file"
    else:
        classification = "repeated_content_new_path"

    paths = sorted({*matching_source.get("paths", []), source_path})
    matching_source.update(
        {
            "last_seen_at_utc": seen_at_utc,
            "run_count": int(matching_source.get("run_count", 0)) + 1,
            "rows": rows,
            "paths": paths,
        }
    )
    history["sources"] = sorted(sources, key=lambda source: source["sha256"])

    return {
        "classification": classification,
        "previously_seen": classification != "new_source_file",
        "run_count_for_source": matching_source["run_count"],
        "known_paths_for_source": paths,
    }


def _date_range(rows):
    dates = sorted({row["order_date"] for row in rows})
    if not dates:
        return {"min": None, "max": None}
    return {"min": dates[0], "max": dates[-1]}


def build_order_watermark(rows):
    if not rows:
        return {"order_date": None, "order_id": None}

    latest_order_date = max(row["order_date"] for row in rows)
    latest_order_id = max(
        row["order_id"]
        for row in rows
        if row["order_date"] == latest_order_date
    )
    return {
        "order_date": latest_order_date,
        "order_id": latest_order_id,
    }


def build_source_profile(rows):
    return {
        "order_date_range": _date_range(rows),
        "high_watermark": build_order_watermark(rows),
        "status_counts": dict(
            sorted(Counter(row["status"] for row in rows).items())
        ),
    }


def build_silver_profile(rows):
    return {
        "order_date_range": _date_range(rows),
        "high_watermark": build_order_watermark(rows),
        "customers": len({row["customer_id"] for row in rows}),
        "categories": len({row["category"] for row in rows}),
        "total_revenue": round(sum(row["revenue"] for row in rows), 2),
    }


def build_run_manifest(
    *,
    config_path,
    config_sha256,
    started_at_utc,
    completed_at_utc,
    duration_ms,
    raw_path,
    source_sha256,
    ingestion_event,
    processed_dir,
    included_statuses,
    order_date_start,
    order_date_end,
    warning_thresholds,
    health_warnings,
    bronze_rows,
    silver_rows,
    rejected_rows,
    gold_rows,
    customer_gold_rows,
    category_gold_rows,
    rejection_gold_rows,
    quality_report,
):
    artifacts = {
        "bronze_orders": processed_dir / "bronze_orders.csv",
        "rejected_orders": processed_dir / "rejected_orders.csv",
        "silver_orders": processed_dir / "silver_orders.csv",
        "gold_revenue_metrics": processed_dir / "gold_revenue_metrics.csv",
        "gold_customer_metrics": processed_dir / "gold_customer_metrics.csv",
        "gold_category_metrics": processed_dir / "gold_category_metrics.csv",
        "gold_rejection_metrics": processed_dir / "gold_rejection_metrics.csv",
        "data_quality_report": processed_dir / "data_quality_report.json",
        "ingestion_history": processed_dir / INGESTION_HISTORY_FILENAME,
    }

    return {
        "generated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "run": {
            "config_path": str(config_path),
            "config_sha256": config_sha256,
            "raw_path": str(raw_path),
            "processed_dir": str(processed_dir),
            "started_at_utc": started_at_utc,
            "completed_at_utc": completed_at_utc,
            "duration_ms": duration_ms,
        },
        "source": {
            "path": str(raw_path),
            "sha256": source_sha256,
            "rows": len(bronze_rows),
            "profile": build_source_profile(bronze_rows),
            "ingestion": ingestion_event,
        },
        "config": {
            "included_statuses": list(included_statuses),
            "order_date_window": {
                "start": order_date_start,
                "end": order_date_end,
            },
            "warning_thresholds": dict(warning_thresholds),
        },
        "health": {
            "warnings": health_warnings,
            "warning_count": len(health_warnings),
        },
        "layers": {
            "bronze": {"rows": len(bronze_rows)},
            "rejected": {
                "rows": len(rejected_rows),
                "reasons": dict(
                    sorted(
                        Counter(
                            row["rejection_reason"] for row in rejected_rows
                        ).items()
                    )
                ),
            },
            "silver": {
                "rows": len(silver_rows),
                "profile": build_silver_profile(silver_rows),
            },
            "gold": {"rows": len(gold_rows)},
            "gold_customer": {"rows": len(customer_gold_rows)},
            "gold_category": {"rows": len(category_gold_rows)},
            "gold_rejection": {"rows": len(rejection_gold_rows)},
        },
        "quality": {
            "success": quality_report["success"],
            "summary": quality_report["summary"],
        },
        "reconciliation": build_row_count_reconciliation(
            bronze_rows,
            silver_rows,
            rejected_rows,
        ),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
    }


def build_silver_outputs(
    rows,
    included_statuses=("delivered",),
    order_date_start=None,
    order_date_end=None,
):
    silver_rows = []
    rejected_rows = []
    included_statuses = set(included_statuses)

    for row in rows:
        if row["status"] not in included_statuses:
            rejected_rows.append({**row, "rejection_reason": "status_not_included"})
            continue
        if not _is_within_order_date_window(
            row,
            start=order_date_start,
            end=order_date_end,
        ):
            rejected_rows.append({**row, "rejection_reason": "order_date_out_of_range"})
            continue

        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        revenue = quantity * unit_price

        silver_rows.append(
            {
                "order_id": row["order_id"],
                "customer_id": row["customer_id"],
                "order_date": row["order_date"],
                "category": row["category"],
                "product": row["product"],
                "quantity": quantity,
                "unit_price": unit_price,
                "revenue": revenue,
            }
        )

    return silver_rows, rejected_rows


def build_row_count_reconciliation(bronze_rows, silver_rows, rejected_rows):
    bronze_count = len(bronze_rows)
    silver_count = len(silver_rows)
    rejected_count = len(rejected_rows)
    accounted_count = silver_count + rejected_count
    return {
        "success": bronze_count == accounted_count,
        "bronze_rows": bronze_count,
        "silver_rows": silver_count,
        "rejected_rows": rejected_count,
        "accounted_rows": accounted_count,
        "difference": bronze_count - accounted_count,
    }


def _rounded_sum(rows, field):
    return round(sum(float(row[field]) for row in rows), 2)


def _integer_sum(rows, field):
    return sum(int(row[field]) for row in rows)


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


def build_metric_reconciliation(
    *,
    silver_rows,
    rejected_rows,
    gold_rows,
    customer_gold_rows,
    category_gold_rows,
    rejection_gold_rows,
):
    silver_orders = len(silver_rows)
    silver_units = _integer_sum(silver_rows, "quantity")
    silver_revenue = _rounded_sum(silver_rows, "revenue")
    rejected_orders = len(rejected_rows)
    rejected_units = _integer_sum(rejected_rows, "quantity")
    rejected_revenue = round(
        sum(int(row["quantity"]) * float(row["unit_price"]) for row in rejected_rows),
        2,
    )

    checks = [
        _metric_check(
            "gold_revenue_orders_match_silver",
            silver_orders,
            _integer_sum(gold_rows, "orders"),
        ),
        _metric_check(
            "gold_revenue_units_match_silver",
            silver_units,
            _integer_sum(gold_rows, "units"),
        ),
        _metric_check(
            "gold_revenue_amount_match_silver",
            silver_revenue,
            _rounded_sum(gold_rows, "revenue"),
        ),
        _metric_check(
            "gold_customer_orders_match_silver",
            silver_orders,
            _integer_sum(customer_gold_rows, "orders"),
        ),
        _metric_check(
            "gold_customer_units_match_silver",
            silver_units,
            _integer_sum(customer_gold_rows, "units"),
        ),
        _metric_check(
            "gold_customer_revenue_match_silver",
            silver_revenue,
            _rounded_sum(customer_gold_rows, "revenue"),
        ),
        _metric_check(
            "gold_category_orders_match_silver",
            silver_orders,
            _integer_sum(category_gold_rows, "orders"),
        ),
        _metric_check(
            "gold_category_units_match_silver",
            silver_units,
            _integer_sum(category_gold_rows, "units"),
        ),
        _metric_check(
            "gold_category_revenue_match_silver",
            silver_revenue,
            _rounded_sum(category_gold_rows, "revenue"),
        ),
        _metric_check(
            "gold_rejection_orders_match_rejected",
            rejected_orders,
            _integer_sum(rejection_gold_rows, "rejected_orders"),
        ),
        _metric_check(
            "gold_rejection_units_match_rejected",
            rejected_units,
            _integer_sum(rejection_gold_rows, "rejected_units"),
        ),
        _metric_check(
            "gold_rejection_revenue_match_rejected",
            rejected_revenue,
            _rounded_sum(rejection_gold_rows, "potential_revenue"),
        ),
    ]
    failed_checks = [check["name"] for check in checks if not check["success"]]
    return {
        "version": 1,
        "success": not failed_checks,
        "failed_checks": failed_checks,
        "checks": checks,
    }


def raise_for_failed_metric_reconciliation(reconciliation):
    if not reconciliation["success"]:
        raise ValueError(
            "Metric reconciliation failed: "
            f"{', '.join(reconciliation['failed_checks'])}"
        )


def load_previous_run_manifest(path):
    path = Path(path)
    if not path.exists():
        return None, "not_found"

    try:
        with path.open(encoding="utf-8") as file:
            manifest = json.load(file)
    except json.JSONDecodeError:
        return None, "invalid_json"

    if not isinstance(manifest, dict):
        return None, "manifest_root_not_object"
    return manifest, None


def build_run_comparison(
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

    layer_names = [
        "bronze",
        "rejected",
        "silver",
        "gold",
        "gold_customer",
        "gold_category",
        "gold_rejection",
    ]

    def layer_rows(manifest, layer_name):
        return manifest.get("layers", {}).get(layer_name, {}).get("rows")

    def rejected_reasons(manifest):
        reasons = manifest.get("layers", {}).get("rejected", {}).get("reasons")
        if not isinstance(reasons, dict):
            return {}
        return reasons

    def source_status_counts(manifest):
        counts = (
            manifest.get("source", {})
            .get("profile", {})
            .get("status_counts")
        )
        if not isinstance(counts, dict):
            return {}
        return counts

    def profile_watermark(manifest, layer_name):
        if layer_name == "source":
            profile = manifest.get("source", {}).get("profile", {})
        else:
            profile = (
                manifest.get("layers", {})
                .get(layer_name, {})
                .get("profile", {})
            )
        if not isinstance(profile, dict):
            return None
        watermark = profile.get("high_watermark")
        if not isinstance(watermark, dict):
            return None
        return {
            "order_date": watermark.get("order_date"),
            "order_id": watermark.get("order_id"),
        }

    row_count_deltas = {}
    for layer_name in layer_names:
        previous_rows = layer_rows(previous_manifest, layer_name)
        current_rows = layer_rows(current_manifest, layer_name)
        delta = None
        if isinstance(previous_rows, int) and isinstance(current_rows, int):
            delta = current_rows - previous_rows
        row_count_deltas[layer_name] = {
            "previous": previous_rows,
            "current": current_rows,
            "delta": delta,
        }

    previous_reasons = rejected_reasons(previous_manifest)
    current_reasons = rejected_reasons(current_manifest)
    rejection_reason_deltas = {}
    for reason in sorted(set(previous_reasons) | set(current_reasons)):
        previous_count = previous_reasons.get(reason)
        current_count = current_reasons.get(reason)
        delta = None
        if isinstance(previous_count, int) and isinstance(current_count, int):
            delta = current_count - previous_count
        rejection_reason_deltas[reason] = {
            "previous": previous_count,
            "current": current_count,
            "delta": delta,
        }

    previous_status_counts = source_status_counts(previous_manifest)
    current_status_counts = source_status_counts(current_manifest)
    status_count_deltas = {}
    for status in sorted(set(previous_status_counts) | set(current_status_counts)):
        previous_count = previous_status_counts.get(status)
        current_count = current_status_counts.get(status)
        delta = None
        if isinstance(previous_count, int) and isinstance(current_count, int):
            delta = current_count - previous_count
        status_count_deltas[status] = {
            "previous": previous_count,
            "current": current_count,
            "delta": delta,
        }

    previous_source_sha = previous_manifest.get("source", {}).get("sha256")
    current_source_sha = current_manifest.get("source", {}).get("sha256")
    previous_config_sha = previous_manifest.get("run", {}).get("config_sha256")
    current_config_sha = current_manifest.get("run", {}).get("config_sha256")
    previous_quality_success = previous_manifest.get("quality", {}).get("success")
    current_quality_success = current_manifest.get("quality", {}).get("success")
    previous_warning_count = previous_manifest.get("health", {}).get("warning_count")
    current_warning_count = current_manifest.get("health", {}).get("warning_count")
    warning_count_delta = None
    if isinstance(previous_warning_count, int) and isinstance(
        current_warning_count,
        int,
    ):
        warning_count_delta = current_warning_count - previous_warning_count

    previous_artifacts = previous_manifest.get("artifact_inventory", {})
    current_artifacts = current_manifest.get("artifact_inventory", {})
    if not isinstance(previous_artifacts, dict):
        previous_artifacts = {}
    if not isinstance(current_artifacts, dict):
        current_artifacts = {}
    artifact_checksum_changes = {}
    for artifact_name in sorted(set(previous_artifacts) | set(current_artifacts)):
        previous_artifact = previous_artifacts.get(artifact_name, {})
        current_artifact = current_artifacts.get(artifact_name, {})
        if not isinstance(previous_artifact, dict):
            previous_artifact = {}
        if not isinstance(current_artifact, dict):
            current_artifact = {}
        previous_sha256 = previous_artifact.get("sha256")
        current_sha256 = current_artifact.get("sha256")
        sha256_changed = None
        if previous_sha256 is not None and current_sha256 is not None:
            sha256_changed = previous_sha256 != current_sha256

        artifact_checksum_changes[artifact_name] = {
            "previous_exists": previous_artifact.get("exists"),
            "current_exists": current_artifact.get("exists"),
            "previous_sha256": previous_sha256,
            "current_sha256": current_sha256,
            "sha256_changed": sha256_changed,
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
        "source_sha256_changed": previous_source_sha != current_source_sha,
        "config_sha256_changed": previous_config_sha != current_config_sha,
        "quality_success_changed": previous_quality_success != current_quality_success,
        "warning_count": {
            "previous": previous_warning_count,
            "current": current_warning_count,
            "delta": warning_count_delta,
        },
        "high_watermarks": {
            layer_name: {
                "previous": profile_watermark(previous_manifest, layer_name),
                "current": profile_watermark(current_manifest, layer_name),
                "changed": (
                    profile_watermark(previous_manifest, layer_name)
                    != profile_watermark(current_manifest, layer_name)
                ),
            }
            for layer_name in ["source", "silver"]
        },
        "row_count_deltas": row_count_deltas,
        "source_status_count_deltas": status_count_deltas,
        "rejection_reason_deltas": rejection_reason_deltas,
        "artifact_checksum_changes": artifact_checksum_changes,
    }


def build_health_warnings(
    bronze_rows,
    silver_rows,
    rejected_rows,
    warning_thresholds=None,
    as_of_date=None,
):
    warning_thresholds = warning_thresholds or {}
    warnings = []
    bronze_count = len(bronze_rows)
    silver_count = len(silver_rows)
    rejected_count = len(rejected_rows)
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

    if "max_source_lag_days" in warning_thresholds:
        threshold = warning_thresholds["max_source_lag_days"]
        order_dates = sorted(row["order_date"] for row in bronze_rows)
        if order_dates:
            latest_order_date = datetime.strptime(order_dates[-1], "%Y-%m-%d").date()
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

    return warnings


def raise_for_failed_reconciliation(reconciliation):
    if not reconciliation["success"]:
        raise ValueError(
            "Row count reconciliation failed: "
            f"{reconciliation['bronze_rows']} bronze rows but "
            f"{reconciliation['accounted_rows']} accounted rows"
        )


def build_silver_orders(
    rows,
    included_statuses=("delivered",),
    order_date_start=None,
    order_date_end=None,
):
    silver_rows, _ = build_silver_outputs(
        rows,
        included_statuses,
        order_date_start,
        order_date_end,
    )
    return silver_rows


def build_gold_revenue(rows, sql_path=GOLD_SQL_PATH):
    return run_gold_revenue_model(rows, sql_path, GOLD_REVENUE_FIELDS)


def build_gold_customer_metrics(rows, sql_path=GOLD_CUSTOMER_SQL_PATH):
    return run_gold_model(rows, sql_path, GOLD_CUSTOMER_FIELDS)


def build_gold_category_metrics(rows, sql_path=GOLD_CATEGORY_SQL_PATH):
    return run_gold_model(rows, sql_path, GOLD_CATEGORY_FIELDS)


def build_gold_rejection_metrics(rows, sql_path=GOLD_REJECTION_SQL_PATH):
    return run_rejected_order_model(rows, sql_path, GOLD_REJECTION_FIELDS)


def write_layer(path, rows, fieldnames):
    write_csv(path, rows, fieldnames)
    LOGGER.info("Wrote %s rows to %s", len(rows), path)


def replace_directory_after_success(target_dir, staged_dir):
    """Replace a directory with a fully written staged directory."""
    target_dir = Path(target_dir)
    staged_dir = Path(staged_dir)
    backup_dir = None

    if target_dir.exists():
        backup_dir = Path(
            tempfile.mkdtemp(
                dir=target_dir.parent,
                prefix=f".{target_dir.name}.backup.",
            )
        )
        backup_dir.rmdir()
        target_dir.rename(backup_dir)

    try:
        staged_dir.rename(target_dir)
    except Exception:
        if backup_dir is not None and backup_dir.exists() and not target_dir.exists():
            backup_dir.rename(target_dir)
        raise
    finally:
        if backup_dir is not None and backup_dir.exists():
            shutil.rmtree(backup_dir)


def make_staged_directory(target_dir):
    target_dir = Path(target_dir)
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    return Path(
        tempfile.mkdtemp(
            dir=target_dir.parent,
            prefix=f".{target_dir.name}.staged.",
        )
    )


def write_partitioned_layer(base_dir, rows, fieldnames, partition_field, filename):
    staged_dir = make_staged_directory(base_dir)

    partitions = {}
    for row in rows:
        partition_value = str(row[partition_field])
        partitions.setdefault(partition_value, []).append(row)

    try:
        for partition_value, partition_rows in sorted(partitions.items()):
            partition_dir = staged_dir / f"{partition_field}={partition_value}"
            write_csv(partition_dir / filename, partition_rows, fieldnames)
            LOGGER.info(
                "Wrote %s rows to staged partition %s",
                len(partition_rows),
                partition_dir,
            )
        replace_directory_after_success(base_dir, staged_dir)
    except Exception:
        shutil.rmtree(staged_dir, ignore_errors=True)
        raise

    return sorted(partitions)


def write_partitioned_parquet_layer(
    base_dir,
    rows,
    schema_fields,
    partition_field,
    filename,
):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Parquet output requires pyarrow. Install requirements-dev.txt."
        ) from exc

    staged_dir = make_staged_directory(base_dir)

    partitions = {}
    for row in rows:
        partition_value = str(row[partition_field])
        partitions.setdefault(partition_value, []).append(row)

    schema = pa.schema(schema_fields)
    try:
        for partition_value, partition_rows in sorted(partitions.items()):
            partition_dir = staged_dir / f"{partition_field}={partition_value}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            parquet_rows = [
                {key: value for key, value in row.items() if key != partition_field}
                for row in partition_rows
            ]
            table = pa.Table.from_pylist(parquet_rows, schema=schema)
            pq.write_table(table, partition_dir / filename)
            LOGGER.info(
                "Wrote %s rows to staged parquet partition %s",
                len(partition_rows),
                partition_dir,
            )
        replace_directory_after_success(base_dir, staged_dir)
    except Exception:
        shutil.rmtree(staged_dir, ignore_errors=True)
        raise

    return sorted(partitions)


def build_partition_inventory(
    rows,
    partition_field,
    csv_base_dir,
    csv_filename,
    parquet_base_dir,
    parquet_filename,
):
    partitions = {}
    for row in rows:
        partition_value = str(row[partition_field])
        partitions.setdefault(partition_value, 0)
        partitions[partition_value] += 1

    return [
        {
            "value": partition_value,
            "rows": row_count,
            "csv_path": str(
                csv_base_dir
                / f"{partition_field}={partition_value}"
                / csv_filename
            ),
            "parquet_path": str(
                parquet_base_dir
                / f"{partition_field}={partition_value}"
                / parquet_filename
            ),
        }
        for partition_value, row_count in sorted(partitions.items())
    ]


def _artifact_file_paths(path):
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(item for item in path.rglob("*") if item.is_file())
    return []


def build_artifact_inventory(artifacts):
    inventory = {}
    for name, path in sorted(artifacts.items()):
        artifact_path = Path(path)
        files = _artifact_file_paths(artifact_path)
        artifact_type = "missing"
        if artifact_path.is_dir():
            artifact_type = "directory"
        elif artifact_path.is_file():
            artifact_type = "file"

        inventory[name] = {
            "path": str(artifact_path),
            "exists": artifact_path.exists(),
            "type": artifact_type,
            "files": len(files),
            "bytes": sum(file_path.stat().st_size for file_path in files),
            "sha256": artifact_sha256(artifact_path),
        }
    return inventory


def _format_summary_value(value):
    if value is None:
        return "n/a"
    return str(value)


def _format_delta(delta):
    if delta is None:
        return "n/a"
    if isinstance(delta, (int, float)) and delta > 0:
        return f"+{delta}"
    return str(delta)


def build_run_summary_markdown(manifest):
    run = manifest.get("run", {})
    source = manifest.get("source", {})
    quality = manifest.get("quality", {})
    health = manifest.get("health", {})
    config = manifest.get("config", {})
    layers = manifest.get("layers", {})
    comparison = manifest.get("run_comparison", {})

    lines = [
        "# Pipeline Run Summary",
        "",
        f"- Completed: {_format_summary_value(run.get('completed_at_utc'))}",
        f"- Source: `{_format_summary_value(source.get('path'))}`",
        f"- Source ingestion: "
        f"`{_format_summary_value(source.get('ingestion', {}).get('classification'))}`",
        f"- Quality: {'passed' if quality.get('success') else 'failed'}",
        f"- Health warnings: {health.get('warning_count', 0)}",
        f"- Config checksum changed: "
        f"{_format_summary_value(comparison.get('config_sha256_changed'))}",
        "",
        "## Config",
        "",
        f"- Included statuses: "
        f"{', '.join(config.get('included_statuses', [])) or 'n/a'}",
        f"- Order date window: "
        f"{_format_summary_value(config.get('order_date_window', {}).get('start'))} "
        f"to {_format_summary_value(config.get('order_date_window', {}).get('end'))}",
        "",
        "## Row Counts",
        "",
        "| Layer | Current rows | Delta |",
        "| --- | ---: | ---: |",
    ]

    row_count_deltas = comparison.get("row_count_deltas", {})
    for layer_name in [
        "bronze",
        "rejected",
        "silver",
        "gold",
        "gold_customer",
        "gold_category",
        "gold_rejection",
    ]:
        current_rows = layers.get(layer_name, {}).get("rows")
        delta = row_count_deltas.get(layer_name, {}).get("delta")
        lines.append(
            f"| {layer_name} | {_format_summary_value(current_rows)} | "
            f"{_format_delta(delta)} |"
        )

    failed_expectations = quality.get("summary", {}).get("failed_expectations", [])
    if failed_expectations:
        lines.extend(
            [
                "",
                "## Failed Quality Expectations",
                "",
                *[f"- `{expectation}`" for expectation in failed_expectations],
            ]
        )

    warnings = health.get("warnings", [])
    if warnings:
        lines.extend(
            [
                "",
                "## Health Warnings",
                "",
                *[
                    f"- `{warning.get('name')}`: {warning.get('message')}"
                    for warning in warnings
                ],
            ]
        )

    artifact_changes = comparison.get("artifact_checksum_changes", {})
    changed_artifacts = [
        artifact_name
        for artifact_name, change in sorted(artifact_changes.items())
        if isinstance(change, dict) and change.get("sha256_changed") is True
    ]
    if changed_artifacts:
        lines.extend(
            [
                "",
                "## Changed Artifacts",
                "",
                *[f"- `{artifact_name}`" for artifact_name in changed_artifacts],
            ]
        )

    lines.append("")
    return "\n".join(lines)


def build_schema_contracts():
    def contract(fields):
        return {
            "columns": [
                {"name": name, "type": field_type}
                for name, field_type in fields.items()
            ]
        }

    return {
        "version": 1,
        "layers": {
            "bronze_orders": contract(BRONZE_FIELD_TYPES),
            "rejected_orders": contract(REJECTED_FIELD_TYPES),
            "silver_orders": contract(SILVER_FIELD_TYPES),
            "gold_revenue_metrics": contract(GOLD_FIELD_TYPES),
            "gold_customer_metrics": contract(GOLD_CUSTOMER_FIELD_TYPES),
            "gold_category_metrics": contract(GOLD_CATEGORY_FIELD_TYPES),
            "gold_rejection_metrics": contract(GOLD_REJECTION_FIELD_TYPES),
        },
    }


def _read_csv_header(path):
    with Path(path).open(newline="", encoding="utf-8") as file:
        return next(csv.reader(file), [])


def build_schema_contract_validation(artifact_paths, schema_contracts):
    layer_results = {}
    for layer_name, contract in sorted(schema_contracts["layers"].items()):
        expected_columns = [column["name"] for column in contract["columns"]]
        artifact_path = Path(artifact_paths[layer_name])
        actual_columns = []
        if artifact_path.is_file():
            actual_columns = _read_csv_header(artifact_path)

        missing_columns = [
            column for column in expected_columns if column not in actual_columns
        ]
        unexpected_columns = [
            column for column in actual_columns if column not in expected_columns
        ]
        layer_success = (
            artifact_path.is_file()
            and actual_columns == expected_columns
            and not missing_columns
            and not unexpected_columns
        )
        layer_results[layer_name] = {
            "success": layer_success,
            "path": str(artifact_path),
            "expected_columns": expected_columns,
            "actual_columns": actual_columns,
            "missing_columns": missing_columns,
            "unexpected_columns": unexpected_columns,
            "order_matches": actual_columns == expected_columns,
        }

    failed_layers = [
        layer_name
        for layer_name, result in layer_results.items()
        if not result["success"]
    ]
    return {
        "version": 1,
        "success": not failed_layers,
        "failed_layers": failed_layers,
        "layers": layer_results,
    }


def raise_for_failed_schema_contract_validation(validation):
    if not validation["success"]:
        raise ValueError(
            "Schema contract validation failed for layers: "
            f"{', '.join(validation['failed_layers'])}"
        )


def _matches_contract_type(value, expected_type):
    if value is None:
        return False
    if expected_type == "string":
        return True
    if expected_type == "date":
        return _is_iso_date(value)
    if expected_type == "integer":
        try:
            int(value)
        except (TypeError, ValueError):
            return False
        return str(value).strip() == str(int(value))
    if expected_type == "float":
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError):
            return False
    raise ValueError(f"Unsupported schema contract type: {expected_type}")


def build_schema_contract_data_validation(
    artifact_paths,
    schema_contracts,
    max_invalid_values=20,
):
    layer_results = {}
    for layer_name, contract in sorted(schema_contracts["layers"].items()):
        artifact_path = Path(artifact_paths[layer_name])
        expected_columns = [column["name"] for column in contract["columns"]]
        expected_types = {
            column["name"]: column["type"] for column in contract["columns"]
        }
        actual_columns = []
        rows_checked = 0
        invalid_value_count = 0
        invalid_values = []

        if artifact_path.is_file():
            with artifact_path.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                actual_columns = reader.fieldnames or []
                if actual_columns == expected_columns:
                    for row_number, row in enumerate(reader, start=2):
                        rows_checked += 1
                        for column, expected_type in expected_types.items():
                            value = row.get(column)
                            if _matches_contract_type(value, expected_type):
                                continue
                            invalid_value_count += 1
                            if len(invalid_values) < max_invalid_values:
                                invalid_values.append(
                                    {
                                        "row_number": row_number,
                                        "column": column,
                                        "value": value,
                                        "expected_type": expected_type,
                                    }
                                )

        layer_results[layer_name] = {
            "success": (
                artifact_path.is_file()
                and actual_columns == expected_columns
                and invalid_value_count == 0
            ),
            "path": str(artifact_path),
            "rows_checked": rows_checked,
            "invalid_value_count": invalid_value_count,
            "invalid_values": invalid_values,
        }

    failed_layers = [
        layer_name
        for layer_name, result in layer_results.items()
        if not result["success"]
    ]
    return {
        "version": 1,
        "success": not failed_layers,
        "failed_layers": failed_layers,
        "max_invalid_values": max_invalid_values,
        "layers": layer_results,
    }


def raise_for_failed_schema_contract_data_validation(validation):
    if not validation["success"]:
        raise ValueError(
            "Schema contract data validation failed for layers: "
            f"{', '.join(validation['failed_layers'])}"
        )


def _normalize_arrow_type(arrow_type):
    arrow_type_name = str(arrow_type)
    if arrow_type_name == "double":
        return "float"
    if arrow_type_name.startswith("int"):
        return "integer"
    return arrow_type_name


def build_partitioned_parquet_contract_validation(
    *,
    parquet_base_dir,
    contract,
    partition_field,
    filename,
):
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Parquet schema validation requires pyarrow. "
            "Install requirements-dev.txt."
        ) from exc

    parquet_base_dir = Path(parquet_base_dir)
    expected_types = {
        column["name"]: column["type"]
        for column in contract["columns"]
        if column["name"] != partition_field
    }
    expected_columns = list(expected_types)
    partition_dirs = sorted(parquet_base_dir.glob(f"{partition_field}=*"))
    partitions = []

    for partition_dir in partition_dirs:
        partition_value = partition_dir.name.split("=", 1)[1]
        parquet_path = partition_dir / filename
        actual_types = {}
        actual_columns = []
        read_error = None
        if parquet_path.is_file():
            try:
                schema = pq.read_schema(parquet_path)
                actual_columns = schema.names
                actual_types = {
                    field.name: _normalize_arrow_type(field.type)
                    for field in schema
                }
            except Exception as exc:  # pragma: no cover - exact pyarrow errors vary.
                read_error = str(exc)

        missing_columns = [
            column for column in expected_columns if column not in actual_columns
        ]
        unexpected_columns = [
            column for column in actual_columns if column not in expected_columns
        ]
        type_mismatches = {
            column: {
                "expected": expected_type,
                "actual": actual_types.get(column),
            }
            for column, expected_type in expected_types.items()
            if column in actual_types and actual_types[column] != expected_type
        }
        partition_success = (
            parquet_path.is_file()
            and read_error is None
            and actual_columns == expected_columns
            and not missing_columns
            and not unexpected_columns
            and not type_mismatches
        )
        partitions.append(
            {
                "success": partition_success,
                "partition_value": partition_value,
                "path": str(parquet_path),
                "expected_columns": expected_columns,
                "actual_columns": actual_columns,
                "missing_columns": missing_columns,
                "unexpected_columns": unexpected_columns,
                "type_mismatches": type_mismatches,
                "read_error": read_error,
                "order_matches": actual_columns == expected_columns,
            }
        )

    failed_partitions = [
        partition["partition_value"]
        for partition in partitions
        if not partition["success"]
    ]
    return {
        "version": 1,
        "success": bool(partitions) and not failed_partitions,
        "artifact": "silver_orders_by_date_parquet",
        "path": str(parquet_base_dir),
        "partition_field": partition_field,
        "file_name": filename,
        "expected_physical_columns": expected_columns,
        "partition_count": len(partitions),
        "failed_partitions": failed_partitions,
        "partitions": partitions,
    }


def raise_for_failed_partitioned_parquet_contract_validation(validation):
    if not validation["success"]:
        raise ValueError(
            "Partitioned Parquet contract validation failed for "
            f"{validation['artifact']}: "
            f"{', '.join(validation['failed_partitions']) or 'no partitions'}"
        )


def build_sql_model_inventory(artifact_paths=None):
    artifact_paths = artifact_paths or {}
    return {
        "version": 1,
        "models": [
            {
                "name": model["name"],
                "path": str(model["path"]),
                "sha256": file_sha256(model["path"]),
                "input_tables": list(model["input_tables"]),
                "output_artifact": model["output_artifact"],
                "output_path": (
                    str(artifact_paths[model["output_artifact"]])
                    if model["output_artifact"] in artifact_paths
                    else None
                ),
                "output_columns": list(model["output_columns"]),
            }
            for model in SQL_MODEL_DEFINITIONS
        ],
    }


def build_lineage(*, raw_path, processed_dir, artifacts):
    artifact_paths = {name: str(path) for name, path in artifacts.items()}
    source_node = {
        "id": "source.raw_orders",
        "type": "source",
        "path": str(raw_path),
    }
    nodes = [
        source_node,
        {
            "id": "quality.raw_order_expectations",
            "type": "quality_report",
            "path": artifact_paths["data_quality_report"],
        },
        {
            "id": "history.source_ingestion",
            "type": "metadata",
            "path": artifact_paths["ingestion_history"],
        },
        {
            "id": "bronze.orders",
            "type": "table",
            "layer": "bronze",
            "path": artifact_paths["bronze_orders"],
        },
        {
            "id": "silver.orders",
            "type": "table",
            "layer": "silver",
            "path": artifact_paths["silver_orders"],
        },
        {
            "id": "silver.orders_by_date_csv",
            "type": "partitioned_table",
            "layer": "silver",
            "format": "csv",
            "path": artifact_paths["silver_orders_by_date"],
        },
        {
            "id": "silver.orders_by_date_parquet",
            "type": "partitioned_table",
            "layer": "silver",
            "format": "parquet",
            "path": artifact_paths["silver_orders_by_date_parquet"],
        },
        {
            "id": "rejected.orders",
            "type": "table",
            "layer": "silver_audit",
            "path": artifact_paths["rejected_orders"],
        },
        {
            "id": "gold.revenue_metrics",
            "type": "sql_model",
            "layer": "gold",
            "model_path": str(GOLD_SQL_PATH),
            "path": artifact_paths["gold_revenue_metrics"],
        },
        {
            "id": "gold.customer_metrics",
            "type": "sql_model",
            "layer": "gold",
            "model_path": str(GOLD_CUSTOMER_SQL_PATH),
            "path": artifact_paths["gold_customer_metrics"],
        },
        {
            "id": "gold.category_metrics",
            "type": "sql_model",
            "layer": "gold",
            "model_path": str(GOLD_CATEGORY_SQL_PATH),
            "path": artifact_paths["gold_category_metrics"],
        },
        {
            "id": "gold.rejection_metrics",
            "type": "sql_model",
            "layer": "gold",
            "model_path": str(GOLD_REJECTION_SQL_PATH),
            "path": artifact_paths["gold_rejection_metrics"],
        },
    ]
    edges = [
        {"from": "source.raw_orders", "to": "quality.raw_order_expectations"},
        {"from": "source.raw_orders", "to": "history.source_ingestion"},
        {"from": "source.raw_orders", "to": "bronze.orders"},
        {"from": "bronze.orders", "to": "silver.orders"},
        {"from": "bronze.orders", "to": "rejected.orders"},
        {"from": "silver.orders", "to": "silver.orders_by_date_csv"},
        {"from": "silver.orders", "to": "silver.orders_by_date_parquet"},
        {"from": "silver.orders", "to": "gold.revenue_metrics"},
        {"from": "silver.orders", "to": "gold.customer_metrics"},
        {"from": "silver.orders", "to": "gold.category_metrics"},
        {"from": "rejected.orders", "to": "gold.rejection_metrics"},
    ]

    return {
        "version": 1,
        "root": str(processed_dir),
        "nodes": nodes,
        "edges": edges,
    }


def main(config_path=DEFAULT_CONFIG_PATH):
    started_at = datetime.now(timezone.utc)
    started_at_monotonic = time.perf_counter()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    config_path = Path(config_path)
    config = load_config(config_path)
    config_sha256 = file_sha256(config_path)
    raw_path = resolve_pipeline_path(config["raw_path"])
    processed_dir = resolve_pipeline_path(config["processed_dir"])

    LOGGER.info("Starting pipeline with source %s", raw_path)
    bronze_rows = read_csv(raw_path)
    source_sha256 = file_sha256(raw_path)
    quality_report = evaluate_quality(
        bronze_rows,
        included_statuses=config["included_statuses"],
        order_date_start=config.get("order_date_start"),
        order_date_end=config.get("order_date_end"),
    )
    quality_report_path = processed_dir / "data_quality_report.json"
    write_json(quality_report_path, quality_report)
    LOGGER.info("Wrote data quality report to %s", quality_report_path)
    raise_for_failed_quality(quality_report)

    write_layer(processed_dir / "bronze_orders.csv", bronze_rows, bronze_rows[0].keys())

    silver_rows, rejected_rows = build_silver_outputs(
        bronze_rows,
        config["included_statuses"],
        order_date_start=config.get("order_date_start"),
        order_date_end=config.get("order_date_end"),
    )
    raise_for_failed_reconciliation(
        build_row_count_reconciliation(bronze_rows, silver_rows, rejected_rows)
    )
    health_warnings = build_health_warnings(
        bronze_rows,
        silver_rows,
        rejected_rows,
        config["warning_thresholds"],
        as_of_date=started_at.date(),
    )
    for warning in health_warnings:
        LOGGER.warning("%s: %s", warning["name"], warning["message"])

    rejected_fields = [*bronze_rows[0].keys(), "rejection_reason"]
    write_layer(
        processed_dir / "rejected_orders.csv",
        rejected_rows,
        rejected_fields,
    )
    silver_fields = [
        "order_id", "customer_id", "order_date", "category", "product",
        "quantity", "unit_price", "revenue",
    ]
    write_layer(processed_dir / "silver_orders.csv", silver_rows, silver_fields)
    silver_partitions = write_partitioned_layer(
        processed_dir / "silver_orders_by_date",
        silver_rows,
        silver_fields,
        "order_date",
        "silver_orders.csv",
    )
    parquet_schema = [
        ("order_id", "string"),
        ("customer_id", "string"),
        ("category", "string"),
        ("product", "string"),
        ("quantity", "int64"),
        ("unit_price", "float64"),
        ("revenue", "float64"),
    ]
    silver_parquet_partitions = write_partitioned_parquet_layer(
        processed_dir / "silver_orders_by_date_parquet",
        silver_rows,
        parquet_schema,
        "order_date",
        "silver_orders.parquet",
    )

    gold_rows = build_gold_revenue(silver_rows)
    write_layer(
        processed_dir / "gold_revenue_metrics.csv",
        gold_rows,
        GOLD_REVENUE_FIELDS,
    )

    customer_gold_rows = build_gold_customer_metrics(silver_rows)
    write_layer(
        processed_dir / "gold_customer_metrics.csv",
        customer_gold_rows,
        GOLD_CUSTOMER_FIELDS,
    )

    category_gold_rows = build_gold_category_metrics(silver_rows)
    write_layer(
        processed_dir / "gold_category_metrics.csv",
        category_gold_rows,
        GOLD_CATEGORY_FIELDS,
    )

    rejection_gold_rows = build_gold_rejection_metrics(rejected_rows)
    write_layer(
        processed_dir / "gold_rejection_metrics.csv",
        rejection_gold_rows,
        GOLD_REJECTION_FIELDS,
    )

    completed_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    ingestion_history_path = processed_dir / INGESTION_HISTORY_FILENAME
    ingestion_history = load_ingestion_history(ingestion_history_path)
    ingestion_event = update_ingestion_history(
        ingestion_history,
        source_path=raw_path,
        source_sha256=source_sha256,
        rows=len(bronze_rows),
        seen_at_utc=completed_at_utc,
    )
    write_json(ingestion_history_path, ingestion_history)
    LOGGER.info(
        "Updated ingestion history at %s with %s",
        ingestion_history_path,
        ingestion_event["classification"],
    )

    manifest = build_run_manifest(
        config_path=config_path.resolve(),
        config_sha256=config_sha256,
        started_at_utc=started_at.isoformat().replace("+00:00", "Z"),
        completed_at_utc=completed_at_utc,
        duration_ms=round((time.perf_counter() - started_at_monotonic) * 1000, 3),
        raw_path=raw_path,
        source_sha256=source_sha256,
        ingestion_event=ingestion_event,
        processed_dir=processed_dir,
        included_statuses=config["included_statuses"],
        order_date_start=config.get("order_date_start"),
        order_date_end=config.get("order_date_end"),
        warning_thresholds=config["warning_thresholds"],
        health_warnings=health_warnings,
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        rejected_rows=rejected_rows,
        gold_rows=gold_rows,
        customer_gold_rows=customer_gold_rows,
        category_gold_rows=category_gold_rows,
        rejection_gold_rows=rejection_gold_rows,
        quality_report=quality_report,
    )
    manifest["artifacts"]["silver_orders_by_date"] = str(
        processed_dir / "silver_orders_by_date"
    )
    manifest["artifacts"]["silver_orders_by_date_parquet"] = str(
        processed_dir / "silver_orders_by_date_parquet"
    )
    manifest["lineage"] = build_lineage(
        raw_path=raw_path,
        processed_dir=processed_dir,
        artifacts={
            name: Path(path)
            for name, path in manifest["artifacts"].items()
        },
    )
    manifest["layers"]["silver"]["partitions"] = {
        "field": "order_date",
        "values": silver_partitions,
    }
    manifest["layers"]["silver"]["parquet_partitions"] = {
        "field": "order_date",
        "values": silver_parquet_partitions,
    }
    manifest["layers"]["silver"]["partition_inventory"] = build_partition_inventory(
        silver_rows,
        "order_date",
        processed_dir / "silver_orders_by_date",
        "silver_orders.csv",
        processed_dir / "silver_orders_by_date_parquet",
        "silver_orders.parquet",
    )
    manifest["artifact_inventory"] = build_artifact_inventory(
        {
            name: Path(path)
            for name, path in manifest["artifacts"].items()
        }
    )
    manifest["schema_contracts"] = build_schema_contracts()
    artifact_paths = {
        name: Path(path)
        for name, path in manifest["artifacts"].items()
    }
    manifest["schema_contract_validation"] = build_schema_contract_validation(
        artifact_paths,
        manifest["schema_contracts"],
    )
    raise_for_failed_schema_contract_validation(
        manifest["schema_contract_validation"]
    )
    manifest["schema_contract_data_validation"] = (
        build_schema_contract_data_validation(
            artifact_paths,
            manifest["schema_contracts"],
        )
    )
    raise_for_failed_schema_contract_data_validation(
        manifest["schema_contract_data_validation"]
    )
    manifest["partition_contract_validation"] = {
        "silver_orders_by_date_parquet": (
            build_partitioned_parquet_contract_validation(
                parquet_base_dir=processed_dir / "silver_orders_by_date_parquet",
                contract=manifest["schema_contracts"]["layers"]["silver_orders"],
                partition_field="order_date",
                filename="silver_orders.parquet",
            )
        )
    }
    raise_for_failed_partitioned_parquet_contract_validation(
        manifest["partition_contract_validation"]["silver_orders_by_date_parquet"]
    )
    manifest["metric_reconciliation"] = build_metric_reconciliation(
        silver_rows=silver_rows,
        rejected_rows=rejected_rows,
        gold_rows=gold_rows,
        customer_gold_rows=customer_gold_rows,
        category_gold_rows=category_gold_rows,
        rejection_gold_rows=rejection_gold_rows,
    )
    raise_for_failed_metric_reconciliation(manifest["metric_reconciliation"])
    manifest["sql_models"] = build_sql_model_inventory(
        artifact_paths
    )
    manifest_path = processed_dir / "pipeline_manifest.json"
    previous_manifest, previous_manifest_unavailable_reason = (
        load_previous_run_manifest(manifest_path)
    )
    manifest["run_comparison"] = build_run_comparison(
        manifest,
        previous_manifest=previous_manifest,
        unavailable_reason=previous_manifest_unavailable_reason,
    )
    write_json(manifest_path, manifest)
    LOGGER.info("Wrote pipeline manifest to %s", manifest_path)
    summary_path = processed_dir / "pipeline_run_summary.md"
    write_text(summary_path, build_run_summary_markdown(manifest))
    LOGGER.info("Wrote pipeline run summary to %s", summary_path)

    LOGGER.info(
        "Pipeline completed: %s raw, %s rejected, %s silver, %s revenue gold, "
        "%s customer gold, %s category gold, and %s rejection gold rows",
        len(bronze_rows),
        len(rejected_rows),
        len(silver_rows),
        len(gold_rows),
        len(customer_gold_rows),
        len(category_gold_rows),
        len(rejection_gold_rows),
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the retail lakehouse pipeline."
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
    main(args.config)


if __name__ == "__main__":
    cli()
