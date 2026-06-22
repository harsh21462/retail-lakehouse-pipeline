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


def run_gold_revenue_model(rows, sql_path):
    """Load silver rows into SQLite and execute the checked-in gold SQL model."""
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
        results = connection.execute(query).fetchall()

    return [dict(row) for row in results]
