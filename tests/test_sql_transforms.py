from src.pipeline import GOLD_CATEGORY_SQL_PATH, GOLD_CUSTOMER_SQL_PATH, GOLD_SQL_PATH
from src.sql_transforms import run_gold_model, run_gold_revenue_model


def test_gold_revenue_model_aggregates_and_orders_silver_rows():
    rows = [
        {
            "order_id": "1002",
            "customer_id": "C002",
            "order_date": "2026-06-02",
            "category": "Home",
            "product": "Chair",
            "quantity": 2,
            "unit_price": 2500.0,
            "revenue": 5000.0,
        },
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": 2,
            "unit_price": 1500.0,
            "revenue": 3000.0,
        },
        {
            "order_id": "1003",
            "customer_id": "C003",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Mouse",
            "quantity": 1,
            "unit_price": 800.0,
            "revenue": 800.0,
        },
    ]

    assert run_gold_revenue_model(rows, GOLD_SQL_PATH) == [
        {
            "order_date": "2026-06-01",
            "category": "Electronics",
            "orders": 2,
            "units": 3,
            "revenue": 3800.0,
            "average_order_value": 1900.0,
        },
        {
            "order_date": "2026-06-02",
            "category": "Home",
            "orders": 1,
            "units": 2,
            "revenue": 5000.0,
            "average_order_value": 5000.0,
        },
    ]


def test_gold_revenue_model_handles_an_empty_silver_layer():
    assert run_gold_revenue_model([], GOLD_SQL_PATH) == []


def test_gold_customer_model_summarizes_customer_value():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": 2,
            "unit_price": 1500.0,
            "revenue": 3000.0,
        },
        {
            "order_id": "1002",
            "customer_id": "C002",
            "order_date": "2026-06-02",
            "category": "Home",
            "product": "Chair",
            "quantity": 2,
            "unit_price": 2500.0,
            "revenue": 5000.0,
        },
        {
            "order_id": "1003",
            "customer_id": "C001",
            "order_date": "2026-06-03",
            "category": "Electronics",
            "product": "Mouse",
            "quantity": 1,
            "unit_price": 800.0,
            "revenue": 800.0,
        },
    ]

    assert run_gold_model(rows, GOLD_CUSTOMER_SQL_PATH) == [
        {
            "customer_id": "C002",
            "orders": 1,
            "units": 2,
            "revenue": 5000.0,
            "first_order_date": "2026-06-02",
            "last_order_date": "2026-06-02",
        },
        {
            "customer_id": "C001",
            "orders": 2,
            "units": 3,
            "revenue": 3800.0,
            "first_order_date": "2026-06-01",
            "last_order_date": "2026-06-03",
        },
    ]


def test_gold_category_model_summarizes_category_performance():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": 2,
            "unit_price": 1500.0,
            "revenue": 3000.0,
        },
        {
            "order_id": "1002",
            "customer_id": "C002",
            "order_date": "2026-06-02",
            "category": "Home",
            "product": "Chair",
            "quantity": 2,
            "unit_price": 2500.0,
            "revenue": 5000.0,
        },
        {
            "order_id": "1003",
            "customer_id": "C001",
            "order_date": "2026-06-03",
            "category": "Electronics",
            "product": "Mouse",
            "quantity": 1,
            "unit_price": 800.0,
            "revenue": 800.0,
        },
    ]

    assert run_gold_model(rows, GOLD_CATEGORY_SQL_PATH) == [
        {
            "category": "Home",
            "orders": 1,
            "customers": 1,
            "units": 2,
            "revenue": 5000.0,
            "average_order_value": 5000.0,
            "first_order_date": "2026-06-02",
            "last_order_date": "2026-06-02",
        },
        {
            "category": "Electronics",
            "orders": 2,
            "customers": 1,
            "units": 3,
            "revenue": 3800.0,
            "average_order_value": 1900.0,
            "first_order_date": "2026-06-01",
            "last_order_date": "2026-06-03",
        },
    ]
