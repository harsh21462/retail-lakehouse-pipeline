import sqlite3
from pathlib import Path


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

REJECTED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "category",
    "product",
    "quantity",
    "unit_price",
    "status",
    "rejection_reason",
]


def _validate_model_columns(sql_path, actual_columns, expected_columns):
    if expected_columns is None:
        return

    expected_columns = list(expected_columns)
    if actual_columns == expected_columns:
        return

    missing_columns = [
        column for column in expected_columns if column not in actual_columns
    ]
    unexpected_columns = [
        column for column in actual_columns if column not in expected_columns
    ]
    raise ValueError(
        "SQL model output contract mismatch for "
        f"{sql_path}: expected columns {expected_columns}, got {actual_columns}; "
        f"missing {missing_columns}, unexpected {unexpected_columns}"
    )


def run_gold_model(rows, sql_path, expected_columns=None):
    """Load silver rows into SQLite and execute a checked-in gold SQL model."""
    query = Path(sql_path).read_text(encoding="utf-8")

    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            create table silver_orders (
                order_id text not null,
                customer_id text not null,
                order_date text not null,
                category text not null,
                product text not null,
                quantity integer not null,
                unit_price real not null,
                revenue real not null
            )
            """
        )
        connection.executemany(
            f"insert into silver_orders ({', '.join(SILVER_COLUMNS)}) "
            f"values ({', '.join('?' for _ in SILVER_COLUMNS)})",
            ([row[column] for column in SILVER_COLUMNS] for row in rows),
        )
        cursor = connection.execute(query)
        actual_columns = [column[0] for column in cursor.description or []]
        _validate_model_columns(sql_path, actual_columns, expected_columns)
        results = cursor.fetchall()

    return [dict(row) for row in results]


def run_gold_revenue_model(rows, sql_path, expected_columns=None):
    return run_gold_model(rows, sql_path, expected_columns)


def run_rejected_order_model(rows, sql_path, expected_columns=None):
    """Load rejected rows into SQLite and execute a checked-in audit SQL model."""
    query = Path(sql_path).read_text(encoding="utf-8")

    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            create table rejected_orders (
                order_id text not null,
                customer_id text not null,
                order_date text not null,
                category text not null,
                product text not null,
                quantity integer not null,
                unit_price real not null,
                status text not null,
                rejection_reason text not null
            )
            """
        )
        connection.executemany(
            f"insert into rejected_orders ({', '.join(REJECTED_COLUMNS)}) "
            f"values ({', '.join('?' for _ in REJECTED_COLUMNS)})",
            ([row[column] for column in REJECTED_COLUMNS] for row in rows),
        )
        cursor = connection.execute(query)
        actual_columns = [column[0] for column in cursor.description or []]
        _validate_model_columns(sql_path, actual_columns, expected_columns)
        results = cursor.fetchall()

    return [dict(row) for row in results]
