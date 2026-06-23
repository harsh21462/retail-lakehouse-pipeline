import csv
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

try:
    from .quality_checks import run_quality_checks
    from .sql_transforms import run_gold_revenue_model
except ImportError:  # Support direct execution with `python src/pipeline.py`.
    from quality_checks import run_quality_checks
    from sql_transforms import run_gold_revenue_model


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config" / "pipeline.json"
GOLD_SQL_PATH = ROOT / "sql" / "gold_revenue_metrics.sql"
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
    gold_rows,
    quality_report,
):
    artifacts = {
        "bronze_orders": processed_dir / "bronze_orders.csv",
        "silver_orders": processed_dir / "silver_orders.csv",
        "gold_revenue_metrics": processed_dir / "gold_revenue_metrics.csv",
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
            "silver": {"rows": len(silver_rows)},
            "gold": {"rows": len(gold_rows)},
        },
        "quality": {
            "success": quality_report["success"],
            "expectations": len(quality_report["expectations"]),
        },
        "artifacts": {name: str(path) for name, path in artifacts.items()},
    }


def build_silver_orders(rows, included_statuses=("delivered",)):
    silver_rows = []
    included_statuses = set(included_statuses)

    for row in rows:
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        revenue = quantity * unit_price

        if row["status"] not in included_statuses:
            continue

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

    return silver_rows


def build_gold_revenue(rows, sql_path=GOLD_SQL_PATH):
    return run_gold_revenue_model(rows, sql_path)


def write_layer(path, rows, fieldnames):
    write_csv(path, rows, fieldnames)
    LOGGER.info("Wrote %s rows to %s", len(rows), path)


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
    quality_report = run_quality_checks(bronze_rows)
    quality_report_path = processed_dir / "data_quality_report.json"
    write_json(quality_report_path, quality_report)
    LOGGER.info("Wrote data quality report to %s", quality_report_path)

    write_layer(processed_dir / "bronze_orders.csv", bronze_rows, bronze_rows[0].keys())

    silver_rows = build_silver_orders(bronze_rows, config["included_statuses"])
    silver_fields = [
        "order_id", "customer_id", "order_date", "category", "product",
        "quantity", "unit_price", "revenue",
    ]
    write_layer(processed_dir / "silver_orders.csv", silver_rows, silver_fields)

    gold_rows = build_gold_revenue(silver_rows)
    gold_fields = [
        "order_date", "category", "orders", "units", "revenue",
        "average_order_value",
    ]
    write_layer(processed_dir / "gold_revenue_metrics.csv", gold_rows, gold_fields)

    manifest = build_run_manifest(
        raw_path=raw_path,
        processed_dir=processed_dir,
        included_statuses=config["included_statuses"],
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        gold_rows=gold_rows,
        quality_report=quality_report,
    )
    manifest_path = processed_dir / "pipeline_manifest.json"
    write_json(manifest_path, manifest)
    LOGGER.info("Wrote pipeline manifest to %s", manifest_path)

    LOGGER.info(
        "Pipeline completed: %s raw, %s silver, and %s gold rows",
        len(bronze_rows), len(silver_rows), len(gold_rows),
    )


if __name__ == "__main__":
    main()
