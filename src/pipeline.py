import csv
from collections import Counter
import hashlib
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

try:
    from .quality_checks import evaluate_quality, raise_for_failed_quality
    from .sql_transforms import run_gold_model, run_gold_revenue_model
except ImportError:  # Support direct execution with `python src/pipeline.py`.
    from quality_checks import evaluate_quality, raise_for_failed_quality
    from sql_transforms import run_gold_model, run_gold_revenue_model


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config" / "pipeline.json"
GOLD_SQL_PATH = ROOT / "sql" / "gold_revenue_metrics.sql"
GOLD_CUSTOMER_SQL_PATH = ROOT / "sql" / "gold_customer_metrics.sql"
LOGGER = logging.getLogger(__name__)


def load_config(path=DEFAULT_CONFIG_PATH):
    with Path(path).open(encoding="utf-8") as file:
        config = json.load(file)

    required = {"raw_path", "processed_dir", "included_statuses"}
    missing = required - set(config)
    if missing:
        raise ValueError(f"Missing configuration keys: {sorted(missing)}")

    return config


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2)
        file.write("\n")


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_run_manifest(
    *,
    raw_path,
    processed_dir,
    included_statuses,
    bronze_rows,
    silver_rows,
    rejected_rows,
    gold_rows,
    customer_gold_rows,
    quality_report,
):
    artifacts = {
        "bronze_orders": processed_dir / "bronze_orders.csv",
        "rejected_orders": processed_dir / "rejected_orders.csv",
        "silver_orders": processed_dir / "silver_orders.csv",
        "gold_revenue_metrics": processed_dir / "gold_revenue_metrics.csv",
        "gold_customer_metrics": processed_dir / "gold_customer_metrics.csv",
        "data_quality_report": processed_dir / "data_quality_report.json",
    }

    return {
        "generated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "source": {
            "path": str(raw_path),
            "sha256": file_sha256(raw_path),
            "rows": len(bronze_rows),
        },
        "config": {
            "included_statuses": list(included_statuses),
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
            "silver": {"rows": len(silver_rows)},
            "gold": {"rows": len(gold_rows)},
            "gold_customer": {"rows": len(customer_gold_rows)},
        },
        "quality": {
            "success": quality_report["success"],
            "expectations": len(quality_report["expectations"]),
        },
        "artifacts": {name: str(path) for name, path in artifacts.items()},
    }


def build_silver_outputs(rows, included_statuses=("delivered",)):
    silver_rows = []
    rejected_rows = []
    included_statuses = set(included_statuses)

    for row in rows:
        if row["status"] not in included_statuses:
            rejected_rows.append({**row, "rejection_reason": "status_not_included"})
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


def build_silver_orders(rows, included_statuses=("delivered",)):
    silver_rows, _ = build_silver_outputs(rows, included_statuses)
    return silver_rows


def build_gold_revenue(rows, sql_path=GOLD_SQL_PATH):
    return run_gold_revenue_model(rows, sql_path)


def build_gold_customer_metrics(rows, sql_path=GOLD_CUSTOMER_SQL_PATH):
    return run_gold_model(rows, sql_path)


def write_layer(path, rows, fieldnames):
    write_csv(path, rows, fieldnames)
    LOGGER.info("Wrote %s rows to %s", len(rows), path)


def write_partitioned_layer(base_dir, rows, fieldnames, partition_field, filename):
    if base_dir.exists():
        shutil.rmtree(base_dir)

    partitions = {}
    for row in rows:
        partition_value = str(row[partition_field])
        partitions.setdefault(partition_value, []).append(row)

    for partition_value, partition_rows in sorted(partitions.items()):
        partition_dir = base_dir / f"{partition_field}={partition_value}"
        write_csv(partition_dir / filename, partition_rows, fieldnames)
        LOGGER.info(
            "Wrote %s rows to partition %s",
            len(partition_rows),
            partition_dir,
        )

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

    if base_dir.exists():
        shutil.rmtree(base_dir)

    partitions = {}
    for row in rows:
        partition_value = str(row[partition_field])
        partitions.setdefault(partition_value, []).append(row)

    schema = pa.schema(schema_fields)
    for partition_value, partition_rows in sorted(partitions.items()):
        partition_dir = base_dir / f"{partition_field}={partition_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        parquet_rows = [
            {key: value for key, value in row.items() if key != partition_field}
            for row in partition_rows
        ]
        table = pa.Table.from_pylist(parquet_rows, schema=schema)
        pq.write_table(table, partition_dir / filename)
        LOGGER.info(
            "Wrote %s rows to parquet partition %s",
            len(partition_rows),
            partition_dir,
        )

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


def main(config_path=DEFAULT_CONFIG_PATH):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    config = load_config(config_path)
    raw_path = ROOT / config["raw_path"]
    processed_dir = ROOT / config["processed_dir"]

    LOGGER.info("Starting pipeline with source %s", raw_path)
    bronze_rows = read_csv(raw_path)
    quality_report = evaluate_quality(bronze_rows)
    quality_report_path = processed_dir / "data_quality_report.json"
    write_json(quality_report_path, quality_report)
    LOGGER.info("Wrote data quality report to %s", quality_report_path)
    raise_for_failed_quality(quality_report)

    write_layer(processed_dir / "bronze_orders.csv", bronze_rows, bronze_rows[0].keys())

    silver_rows, rejected_rows = build_silver_outputs(
        bronze_rows,
        config["included_statuses"],
    )
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

    manifest = build_run_manifest(
        raw_path=raw_path,
        processed_dir=processed_dir,
        included_statuses=config["included_statuses"],
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        rejected_rows=rejected_rows,
        gold_rows=gold_rows,
        customer_gold_rows=customer_gold_rows,
        quality_report=quality_report,
    )
    manifest["artifacts"]["silver_orders_by_date"] = str(
        processed_dir / "silver_orders_by_date"
    )
    manifest["artifacts"]["silver_orders_by_date_parquet"] = str(
        processed_dir / "silver_orders_by_date_parquet"
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
    manifest_path = processed_dir / "pipeline_manifest.json"
    write_json(manifest_path, manifest)
    LOGGER.info("Wrote pipeline manifest to %s", manifest_path)

    LOGGER.info(
        "Pipeline completed: %s raw, %s rejected, %s silver, %s revenue gold, "
        "and %s customer gold rows",
        len(bronze_rows),
        len(rejected_rows),
        len(silver_rows),
        len(gold_rows),
        len(customer_gold_rows),
    )


if __name__ == "__main__":
    main()
