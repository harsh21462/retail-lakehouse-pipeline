import pytest

from src.pipeline import (
    build_artifact_inventory,
    build_health_warnings,
    load_ingestion_history,
    build_row_count_reconciliation,
    build_silver_orders,
    build_silver_outputs,
    build_silver_profile,
    build_source_profile,
    load_config,
    raise_for_failed_reconciliation,
    resolve_pipeline_path,
    update_ingestion_history,
    write_json,
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


def test_quality_checks_validate_included_status_coverage():
    rows = [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500",
            "status": "cancelled",
        }
    ]

    report = evaluate_quality(rows, included_statuses=["delivered"])
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is False
    assert results["included_statuses_match_source_rows"]["observed"] == {
        "included_statuses": ["delivered"],
        "matching_rows": 0,
        "matching_status_counts": {},
    }


def test_quality_checks_report_matching_included_status_counts():
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
            "order_date": "2026-06-01",
            "category": "Home",
            "product": "Chair",
            "quantity": "1",
            "unit_price": "800",
            "status": "shipped",
        },
        {
            "order_id": "1003",
            "customer_id": "C003",
            "order_date": "2026-06-02",
            "category": "Home",
            "product": "Desk",
            "quantity": "1",
            "unit_price": "1200",
            "status": "cancelled",
        },
    ]

    report = evaluate_quality(rows, included_statuses=["delivered", "shipped"])
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is True
    assert results["included_statuses_match_source_rows"]["observed"] == {
        "included_statuses": ["delivered", "shipped"],
        "matching_rows": 2,
        "matching_status_counts": {"delivered": 1, "shipped": 1},
    }


def test_quality_checks_validate_date_window_selection():
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

    report = evaluate_quality(
        rows,
        included_statuses=["delivered"],
        order_date_start="2026-06-02",
        order_date_end="2026-06-03",
    )
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is False
    assert results["selected_rows_match_config"]["observed"] == {
        "included_statuses": ["delivered"],
        "order_date_start": "2026-06-02",
        "order_date_end": "2026-06-03",
        "matching_rows": 0,
        "matching_status_counts": {},
    }


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


def test_silver_outputs_reject_rows_outside_configured_date_window():
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
            "status": "delivered",
        },
    ]

    silver_rows, rejected_rows = build_silver_outputs(
        rows,
        ["delivered"],
        order_date_start="2026-06-02",
        order_date_end="2026-06-02",
    )

    assert [row["order_id"] for row in silver_rows] == ["1002"]
    assert rejected_rows == [
        {
            **rows[0],
            "rejection_reason": "order_date_out_of_range",
        }
    ]


def test_row_count_reconciliation_accounts_for_silver_and_rejected_rows():
    reconciliation = build_row_count_reconciliation(
        bronze_rows=[{"order_id": "1001"}, {"order_id": "1002"}],
        silver_rows=[{"order_id": "1001"}],
        rejected_rows=[{"order_id": "1002"}],
    )

    assert reconciliation == {
        "success": True,
        "bronze_rows": 2,
        "silver_rows": 1,
        "rejected_rows": 1,
        "accounted_rows": 2,
        "difference": 0,
    }


def test_row_count_reconciliation_fails_on_unaccounted_rows():
    reconciliation = build_row_count_reconciliation(
        bronze_rows=[{"order_id": "1001"}, {"order_id": "1002"}],
        silver_rows=[{"order_id": "1001"}],
        rejected_rows=[],
    )

    assert reconciliation["success"] is False
    assert reconciliation["difference"] == 1
    with pytest.raises(ValueError, match="Row count reconciliation failed"):
        raise_for_failed_reconciliation(reconciliation)


def test_health_warnings_report_threshold_breaches():
    warnings = build_health_warnings(
        bronze_rows=[{"order_id": "1001"}, {"order_id": "1002"}],
        silver_rows=[{"order_id": "1001"}],
        rejected_rows=[{"order_id": "1002"}],
        warning_thresholds={"max_rejection_rate": 0.25, "min_silver_rows": 2},
    )

    assert warnings == [
        {
            "name": "rejection_rate_above_threshold",
            "severity": "warning",
            "message": "Rejected row rate exceeded configured warning threshold",
            "observed": {
                "bronze_rows": 2,
                "rejected_rows": 1,
                "rejection_rate": 0.5,
            },
            "threshold": {"max_rejection_rate": 0.25},
        },
        {
            "name": "silver_rows_below_threshold",
            "severity": "warning",
            "message": "Silver row count fell below configured warning threshold",
            "observed": {"silver_rows": 1},
            "threshold": {"min_silver_rows": 2},
        },
    ]


def test_health_warnings_stay_empty_when_thresholds_are_not_breached():
    warnings = build_health_warnings(
        bronze_rows=[{"order_id": "1001"}, {"order_id": "1002"}],
        silver_rows=[{"order_id": "1001"}, {"order_id": "1002"}],
        rejected_rows=[],
        warning_thresholds={"max_rejection_rate": 0.25, "min_silver_rows": 2},
    )

    assert warnings == []


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


def test_ingestion_history_classifies_new_and_repeated_sources():
    history = {"version": 1, "sources": []}

    first_event = update_ingestion_history(
        history,
        source_path="/data/raw/orders.csv",
        source_sha256="abc123",
        rows=10,
        seen_at_utc="2026-07-16T01:00:00Z",
    )
    second_event = update_ingestion_history(
        history,
        source_path="/data/raw/orders.csv",
        source_sha256="abc123",
        rows=10,
        seen_at_utc="2026-07-16T02:00:00Z",
    )
    renamed_event = update_ingestion_history(
        history,
        source_path="/data/backfill/orders-copy.csv",
        source_sha256="abc123",
        rows=10,
        seen_at_utc="2026-07-16T03:00:00Z",
    )

    assert first_event == {
        "classification": "new_source_file",
        "previously_seen": False,
        "run_count_for_source": 1,
        "known_paths_for_source": ["/data/raw/orders.csv"],
    }
    assert second_event["classification"] == "repeated_source_file"
    assert renamed_event == {
        "classification": "repeated_content_new_path",
        "previously_seen": True,
        "run_count_for_source": 3,
        "known_paths_for_source": [
            "/data/backfill/orders-copy.csv",
            "/data/raw/orders.csv",
        ],
    }
    assert history["sources"] == [
        {
            "sha256": "abc123",
            "first_seen_at_utc": "2026-07-16T01:00:00Z",
            "last_seen_at_utc": "2026-07-16T03:00:00Z",
            "run_count": 3,
            "rows": 10,
            "paths": [
                "/data/backfill/orders-copy.csv",
                "/data/raw/orders.csv",
            ],
        }
    ]


def test_load_ingestion_history_rejects_unknown_format(tmp_path):
    history_path = tmp_path / "ingestion_history.json"
    write_json(history_path, {"version": 99, "sources": []})

    with pytest.raises(ValueError, match="Unsupported ingestion history format"):
        load_ingestion_history(history_path)


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


def test_load_config_validates_optional_order_date_window(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": ["delivered"], '
        '"order_date_start": "2026-06-03", "order_date_end": "2026-06-01"}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="on or before"):
        load_config(config_path)

    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": ["delivered"], "order_date_start": "06/01/2026"}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        load_config(config_path)


def test_load_config_validates_optional_warning_thresholds(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": ["delivered"], '
        '"warning_thresholds": {"max_rejection_rate": 1.5}}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_rejection_rate"):
        load_config(config_path)

    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": ["delivered"], '
        '"warning_thresholds": {"min_silver_rows": true}}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="min_silver_rows"):
        load_config(config_path)

    config_path.write_text(
        '{"raw_path": "orders.csv", "processed_dir": "processed", '
        '"included_statuses": ["delivered"], '
        '"warning_thresholds": {"unexpected": 1}}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported"):
        load_config(config_path)


def test_resolve_pipeline_path_anchors_relative_paths_to_project_root(tmp_path):
    assert resolve_pipeline_path("data/raw/orders.csv", base_dir=tmp_path) == (
        tmp_path / "data" / "raw" / "orders.csv"
    )


def test_resolve_pipeline_path_preserves_absolute_paths(tmp_path):
    absolute_path = tmp_path / "orders.csv"

    assert resolve_pipeline_path(absolute_path, base_dir=tmp_path / "ignored") == (
        absolute_path
    )


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


def test_quality_report_rejects_unexpected_raw_columns():
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
            "coupon_code": "WELCOME10",
        }
    ]

    report = evaluate_quality(rows)
    results = {item["expectation"]: item for item in report["expectations"]}

    assert report["success"] is False
    assert results["raw_schema_matches_contract"]["observed"] == {
        "required_columns": [
            "order_id",
            "customer_id",
            "order_date",
            "category",
            "product",
            "quantity",
            "unit_price",
            "status",
        ],
        "unexpected_columns": ["coupon_code"],
    }
    assert results["rows_are_well_formed"]["success"] is True
