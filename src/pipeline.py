import csv
import json
import logging
from collections import defaultdict
from pathlib import Path

try:
    from .quality_checks import run_quality_checks
except ImportError:  # Support direct execution with `python src/pipeline.py`.
    from quality_checks import run_quality_checks


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "config" / "pipeline.json"
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


def build_gold_revenue(rows):
    metrics = defaultdict(lambda: {"orders": 0, "units": 0, "revenue": 0.0})

    for row in rows:
        key = (row["order_date"], row["category"])
        metrics[key]["orders"] += 1
        metrics[key]["units"] += row["quantity"]
        metrics[key]["revenue"] += row["revenue"]

    gold_rows = []
    for (order_date, category), values in sorted(metrics.items()):
        gold_rows.append(
            {
                "order_date": order_date,
                "category": category,
                "orders": values["orders"],
                "units": values["units"],
                "revenue": round(values["revenue"], 2),
                "average_order_value": round(values["revenue"] / values["orders"], 2),
            }
        )

    return gold_rows


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

    LOGGER.info(
        "Pipeline completed: %s raw, %s silver, and %s gold rows",
        len(bronze_rows), len(silver_rows), len(gold_rows),
    )


if __name__ == "__main__":
    main()
