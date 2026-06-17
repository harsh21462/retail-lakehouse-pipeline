def check_required_columns(rows, required_columns):
    if not rows:
        raise ValueError("Dataset is empty.")

    missing = set(required_columns) - set(rows[0].keys())
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def check_no_duplicate_orders(rows):
    order_ids = [row["order_id"] for row in rows]
    duplicates = {order_id for order_id in order_ids if order_ids.count(order_id) > 1}
    if duplicates:
        raise ValueError(f"Duplicate order_id values found: {sorted(duplicates)}")


def check_positive_amounts(rows):
    invalid = [
        row["order_id"]
        for row in rows
        if int(row["quantity"]) <= 0 or float(row["unit_price"]) <= 0
    ]
    if invalid:
        raise ValueError(f"Invalid quantity or unit_price for orders: {invalid}")


def run_quality_checks(rows):
    required_columns = [
        "order_id",
        "customer_id",
        "order_date",
        "category",
        "product",
        "quantity",
        "unit_price",
        "status",
    ]

    check_required_columns(rows, required_columns)
    check_no_duplicate_orders(rows)
    check_positive_amounts(rows)
