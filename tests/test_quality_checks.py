import pytest

from src.pipeline import (
    build_artifact_inventory,
    build_silver_orders,
    build_silver_outputs,
    build_silver_profile,
    build_source_profile,
    load_config,
)
from src.quality_checks import evaluate_quality, run_quality_checks


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

    report = run_quality_checks(rows)

    assert report["success"] is True
    assert report["row_count"] == 1
    assert all(result["success"] for result in report["expectations"])


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


def test_silver_outputs_return_rejected_rows_with_reason():
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
            "order_id": "1002",
            "customer_id": "C002",
            "order_date": "2026-06-02",
            "category": "Home",
            "product": "Chair",
            "quantity": "1",
            "unit_price": "2500",
            "status": "returned",
        },
    ]

    silver_rows, rejected_rows = build_silver_outputs(rows, ["delivered"])

    assert [row["order_id"] for row in silver_rows] == ["1001"]
    assert rejected_rows == [
        {
            **rows[1],
            "rejection_reason": "status_not_included",
        }
    ]


def test_manifest_profiles_handle_empty_and_populated_rows():
    raw_rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-02",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500",
            "status": "delivered",
        },
        {
            "order_id": "1002",
            "customer_id": "C002",
            "order_date": "2026-06-01",
            "category": "Home",
            "product": "Chair",
            "quantity": "1",
            "unit_price": "2500",
            "status": "returned",
        },
    ]
    silver_rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-02",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": 2,
            "unit_price": 1500.0,
            "revenue": 3000.0,
        }
    ]

    assert build_source_profile(raw_rows) == {
        "order_date_range": {"min": "2026-06-01", "max": "2026-06-02"},
        "status_counts": {"delivered": 1, "returned": 1},
    }
    assert build_silver_profile(silver_rows) == {
        "order_date_range": {"min": "2026-06-02", "max": "2026-06-02"},
        "customers": 1,
        "categories": 1,
        "total_revenue": 3000.0,
    }
    assert build_source_profile([]) == {
        "order_date_range": {"min": None, "max": None},
        "status_counts": {},
    }
    assert build_silver_profile([]) == {
        "order_date_range": {"min": None, "max": None},
        "customers": 0,
        "categories": 0,
        "total_revenue": 0,
    }


def test_artifact_inventory_records_files_directories_and_missing_paths(tmp_path):
    file_path = tmp_path / "silver_orders.csv"
    file_path.write_text("order_id\n1001\n", encoding="utf-8")
    directory_path = tmp_path / "silver_orders_by_date"
    partition_path = directory_path / "order_date=2026-06-01"
    partition_path.mkdir(parents=True)
    (partition_path / "silver_orders.csv").write_text(
        "order_id\n1001\n",
        encoding="utf-8",
    )

    inventory = build_artifact_inventory(
        {
            "silver_orders": file_path,
            "silver_orders_by_date": directory_path,
            "missing_report": tmp_path / "missing.json",
        }
    )

    assert inventory["silver_orders"] == {
        "path": str(file_path),
        "exists": True,
        "type": "file",
        "files": 1,
        "bytes": file_path.stat().st_size,
    }
    assert inventory["silver_orders_by_date"] == {
        "path": str(directory_path),
        "exists": True,
        "type": "directory",
        "files": 1,
        "bytes": (partition_path / "silver_orders.csv").stat().st_size,
    }
    assert inventory["missing_report"] == {
        "path": str(tmp_path / "missing.json"),
        "exists": False,
        "type": "missing",
        "files": 0,
        "bytes": 0,
    }


def test_load_config_requires_all_keys(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text('{"raw_path": "orders.csv"}', encoding="utf-8")

    with pytest.raises(ValueError, match="included_statuses"):
        load_config(config_path)


def test_load_config_rejects_non_list_included_statuses(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": "delivered"}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="non-empty list"):
        load_config(config_path)


def test_load_config_rejects_blank_and_duplicate_statuses(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": ["delivered", " ", "delivered"]}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="non-empty list"):
        load_config(config_path)

    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": ["delivered", "delivered"]}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate"):
        load_config(config_path)


def test_load_config_rejects_blank_paths(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        '{"raw_path": "", "processed_dir": "processed", '
        '"included_statuses": ["delivered"]}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="non-empty strings"):
        load_config(config_path)


def test_quality_report_identifies_all_failed_expectations():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "not-a-number",
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

    report = evaluate_quality(rows)
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is False
    assert results["order_id_is_unique"]["observed"] == {
        "duplicate_order_ids": ["1001"]
    }
    assert results["amounts_are_positive_numbers"]["observed"] == {
        "invalid_order_ids": ["1001"]
    }


def test_quality_report_rejects_blank_order_ids():
    rows = [
        {
            "order_id": " ",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500",
            "status": "delivered",
        },
        {
            "order_id": "",
            "customer_id": "C002",
            "order_date": "2026-06-01",
            "category": "Home",
            "product": "Chair",
            "quantity": "1",
            "unit_price": "800",
            "status": "delivered",
        },
    ]

    report = evaluate_quality(rows)
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is False
    assert results["order_ids_are_populated"]["observed"] == {
        "invalid_order_ids": ["row_1", "row_2"]
    }
    assert results["order_id_is_unique"]["observed"] == {
        "duplicate_order_ids": []
    }


def test_quality_report_rejects_invalid_dates_and_blank_dimensions():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "06/01/2026",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500",
            "status": "delivered",
        },
        {
            "order_id": "1002",
            "customer_id": " ",
            "order_date": "2026-06-01",
            "category": "Home",
            "product": "Chair",
            "quantity": "1",
            "unit_price": "800",
            "status": "delivered",
        },
    ]

    report = evaluate_quality(rows)
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is False
    assert results["order_dates_are_iso_dates"]["observed"] == {
        "invalid_order_ids": ["1001"]
    }
    assert results["business_dimensions_are_populated"]["observed"] == {
        "invalid_order_ids": ["1002"]
    }


def test_quality_report_rejects_malformed_csv_rows():
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
            None: ["unexpected"],
        },
        {
            "order_id": "1002",
            "customer_id": None,
            "order_date": "2026-06-01",
            "category": "Home",
            "product": "Chair",
            "quantity": "1",
            "unit_price": "800",
            "status": "delivered",
        },
    ]

    report = evaluate_quality(rows)
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is False
    assert results["rows_are_well_formed"]["observed"] == {
        "extra_field_order_ids": ["1001"],
        "missing_field_order_ids": ["1002"],
    }
    assert results["business_dimensions_are_populated"]["observed"] == {
        "invalid_order_ids": ["1002"]
    }
