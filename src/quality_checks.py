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


def _is_blank(value):
    return value is None or not str(value).strip()


def _row_identifier(row, index):
    order_id = row.get("order_id")
    if _is_blank(order_id):
        return f"row_{index}"
    return order_id


def evaluate_quality(rows, included_statuses=None):
    """Evaluate the raw order data and return a machine-readable report."""
    columns = list(rows[0]) if rows else []
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(columns))
    unexpected_columns = sorted(
        column for column in set(columns) - set(REQUIRED_COLUMNS) if column is not None
    )
    extra_field_ids = []
    missing_field_ids = []
    if not missing_columns:
        for index, row in enumerate(rows, start=1):
            row_id = _row_identifier(row, index)
            if None in row:
                extra_field_ids.append(row_id)
            if any(row.get(column) is None for column in REQUIRED_COLUMNS):
                missing_field_ids.append(row_id)

    blank_order_id_ids = []
    if not missing_columns:
        for index, row in enumerate(rows, start=1):
            if _is_blank(row["order_id"]):
                blank_order_id_ids.append(_row_identifier(row, index))

    duplicate_ids = sorted(
        order_id
        for order_id, count in Counter(row.get("order_id") for row in rows).items()
        if not _is_blank(order_id) and count > 1
    )

    invalid_amount_ids = []
    if not missing_columns:
        for index, row in enumerate(rows, start=1):
            try:
                valid = int(row["quantity"]) > 0 and float(row["unit_price"]) > 0
            except (TypeError, ValueError):
                valid = False
            if not valid:
                invalid_amount_ids.append(_row_identifier(row, index))

    invalid_date_ids = []
    blank_dimension_ids = []
    if not missing_columns:
        dimension_columns = ["customer_id", "category", "product", "status"]
        for index, row in enumerate(rows, start=1):
            row_id = _row_identifier(row, index)
            if not _is_iso_date(row["order_date"]):
                invalid_date_ids.append(row_id)
            if any(_is_blank(row[column]) for column in dimension_columns):
                blank_dimension_ids.append(row_id)

    status_coverage_expectation = []
    if included_statuses is not None:
        included_status_set = set(included_statuses)
        status_counts = Counter(
            row.get("status")
            for row in rows
            if row.get("status") in included_status_set
        )
        matching_rows = sum(status_counts.values())
        status_coverage_expectation.append(
            _expectation(
                "included_statuses_match_source_rows",
                not missing_columns and matching_rows > 0,
                {
                    "included_statuses": list(included_statuses),
                    "matching_rows": matching_rows,
                    "matching_status_counts": dict(sorted(status_counts.items())),
                },
            )
        )

    expectations = [
        _expectation("dataset_is_not_empty", bool(rows), {"row_count": len(rows)}),
        _expectation(
            "required_columns_are_present",
            not missing_columns,
            {"missing_columns": missing_columns},
        ),
        _expectation(
            "raw_schema_matches_contract",
            not missing_columns and not unexpected_columns,
            {
                "required_columns": REQUIRED_COLUMNS,
                "unexpected_columns": unexpected_columns,
            },
        ),
        _expectation(
            "rows_are_well_formed",
            not missing_columns and not extra_field_ids and not missing_field_ids,
            {
                "extra_field_order_ids": extra_field_ids,
                "missing_field_order_ids": missing_field_ids,
            },
        ),
        _expectation(
            "order_ids_are_populated",
            not missing_columns and not blank_order_id_ids,
            {"invalid_order_ids": blank_order_id_ids},
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
        *status_coverage_expectation,
    ]
    return {
        "success": all(result["success"] for result in expectations),
        "row_count": len(rows),
        "expectations": expectations,
    }


def run_quality_checks(rows, included_statuses=None):
    report = evaluate_quality(rows, included_statuses)
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
