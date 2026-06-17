import csv
from collections import defaultdict
from pathlib import Path

from quality_checks import run_quality_checks


ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "orders.csv"
PROCESSED_DIR = ROOT / "data" / "processed"


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_silver_orders(rows):
    silver_rows = []

    for row in rows:
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        revenue = quantity * unit_price

        if row["status"] != "delivered":
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


def main():
    bronze_rows = read_csv(RAW_PATH)
    run_quality_checks(bronze_rows)

    write_csv(PROCESSED_DIR / "bronze_orders.csv", bronze_rows, bronze_rows[0].keys())

    silver_rows = build_silver_orders(bronze_rows)
    write_csv(PROCESSED_DIR / "silver_orders.csv", silver_rows, silver_rows[0].keys())

    gold_rows = build_gold_revenue(silver_rows)
    write_csv(PROCESSED_DIR / "gold_revenue_metrics.csv", gold_rows, gold_rows[0].keys())

    print(f"Pipeline completed. Processed {len(bronze_rows)} raw rows.")


if __name__ == "__main__":
    main()
