from collections import Counter
from datetime import datetime


REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "category",
    "product",
    "quantity",
    "unit_price",
    "status",
]


def _expectation(name, success, observed):
    return {"expectation": name, "success": success, "observed": observed}


def _is_iso_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat() == value
    except (TypeError, ValueError):
        return False


def evaluate_quality(rows):
    """Evaluate the raw order data and return a machine-readable report."""
    columns = list(rows[0]) if rows else []
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(columns))
    duplicate_ids = sorted(
        order_id
        for order_id, count in Counter(row.get("order_id") for row in rows).items()
        if order_id is not None and count > 1
    )

    invalid_amount_ids = []
    if not missing_columns:
        for row in rows:
            try:
                valid = int(row["quantity"]) > 0 and float(row["unit_price"]) > 0
            except (TypeError, ValueError):
                valid = False
            if not valid:
                invalid_amount_ids.append(row["order_id"])

    invalid_date_ids = []
    blank_dimension_ids = []
    if not missing_columns:
        dimension_columns = ["customer_id", "category", "product", "status"]
        for row in rows:
            if not _is_iso_date(row["order_date"]):
                invalid_date_ids.append(row["order_id"])
            if any(not str(row[column]).strip() for column in dimension_columns):
                blank_dimension_ids.append(row["order_id"])

    expectations = [
        _expectation("dataset_is_not_empty", bool(rows), {"row_count": len(rows)}),
        _expectation(
            "required_columns_are_present",
            not missing_columns,
            {"missing_columns": missing_columns},
        ),
        _expectation(
            "order_id_is_unique",
            not duplicate_ids,
            {"duplicate_order_ids": duplicate_ids},
        ),
        _expectation(
            "amounts_are_positive_numbers",
            not missing_columns and not invalid_amount_ids,
            {"invalid_order_ids": invalid_amount_ids},
        ),
        _expectation(
            "order_dates_are_iso_dates",
            not missing_columns and not invalid_date_ids,
            {"invalid_order_ids": invalid_date_ids},
        ),
        _expectation(
            "business_dimensions_are_populated",
            not missing_columns and not blank_dimension_ids,
            {"invalid_order_ids": blank_dimension_ids},
        ),
    ]
    return {
        "success": all(result["success"] for result in expectations),
        "row_count": len(rows),
        "expectations": expectations,
    }


def run_quality_checks(rows):
    report = evaluate_quality(rows)
    raise_for_failed_quality(report)
    return report


def raise_for_failed_quality(report):
    failed = [
        result["expectation"]
        for result in report["expectations"]
        if not result["success"]
    ]
    if failed:
        raise ValueError(f"Data quality checks failed: {', '.join(failed)}")
