import argparse
import csv
from collections import Counter
import hashlib
import json
import logging
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


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
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


def build_source_profile(rows):
    return {
        "order_date_range": _date_range(rows),
        "status_counts": dict(
            sorted(Counter(row["status"] for row in rows).items())
        ),
    }


def build_silver_profile(rows):
    return {
        "order_date_range": _date_range(rows),
        "customers": len({row["customer_id"] for row in rows}),
        "categories": len({row["category"] for row in rows}),
        "total_revenue": round(sum(row["revenue"] for row in rows), 2),
    }


def build_run_manifest(
    *,
    config_path,
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
            "expectations": len(quality_report["expectations"]),
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
    return run_gold_revenue_model(rows, sql_path)


def build_gold_customer_metrics(rows, sql_path=GOLD_CUSTOMER_SQL_PATH):
    return run_gold_model(rows, sql_path)


def build_gold_category_metrics(rows, sql_path=GOLD_CATEGORY_SQL_PATH):
    return run_gold_model(rows, sql_path)


def build_gold_rejection_metrics(rows, sql_path=GOLD_REJECTION_SQL_PATH):
    return run_rejected_order_model(rows, sql_path)


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
        }
    return inventory


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
    gold_fields = [
        "order_date", "category", "orders", "units", "revenue",
        "average_order_value",
    ]
    write_layer(processed_dir / "gold_revenue_metrics.csv", gold_rows, gold_fields)

    customer_gold_rows = build_gold_customer_metrics(silver_rows)
    customer_gold_fields = [
        "customer_id", "orders", "units", "revenue", "first_order_date",
        "last_order_date",
    ]
    write_layer(
        processed_dir / "gold_customer_metrics.csv",
        customer_gold_rows,
        customer_gold_fields,
    )

    category_gold_rows = build_gold_category_metrics(silver_rows)
    category_gold_fields = [
        "category", "orders", "customers", "units", "revenue",
        "average_order_value", "first_order_date", "last_order_date",
    ]
    write_layer(
        processed_dir / "gold_category_metrics.csv",
        category_gold_rows,
        category_gold_fields,
    )

    rejection_gold_rows = build_gold_rejection_metrics(rejected_rows)
    rejection_gold_fields = [
        "rejection_reason", "status", "order_date", "category",
        "rejected_orders", "rejected_units", "potential_revenue",
    ]
    write_layer(
        processed_dir / "gold_rejection_metrics.csv",
        rejection_gold_rows,
        rejection_gold_fields,
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
    manifest_path = processed_dir / "pipeline_manifest.json"
    write_json(manifest_path, manifest)
    LOGGER.info("Wrote pipeline manifest to %s", manifest_path)

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
