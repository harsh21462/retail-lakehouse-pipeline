import pytest

from src.pipeline import build_silver_orders, load_config
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


def test_silver_orders_use_configured_statuses():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500",
            "status": "shipped",
        }
    ]

    assert build_silver_orders(rows) == []
    assert build_silver_orders(rows, ["shipped"])[0]["revenue"] == 3000.0


def test_load_config_requires_all_keys(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text('{"raw_path": "orders.csv"}', encoding="utf-8")

    with pytest.raises(ValueError, match="included_statuses"):
        load_config(config_path)
