import pytest

from src.quality_checks import run_quality_checks


def test_quality_checks_pass_for_valid_rows():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500",
            "status": "delivered",
        }
    ]

    run_quality_checks(rows)


def test_quality_checks_fail_for_duplicate_order_id():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500",
            "status": "delivered",
        },
        {
            "order_id": "1001",
            "customer_id": "C002",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Mouse",
            "quantity": "1",
            "unit_price": "800",
            "status": "delivered",
        },
    ]

    with pytest.raises(ValueError):
        run_quality_checks(rows)
