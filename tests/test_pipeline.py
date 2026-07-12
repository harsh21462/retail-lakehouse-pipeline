import csv
import hashlib
import json

import pyarrow.parquet as pq
import pytest

from src.pipeline import (
    DEFAULT_CONFIG_PATH,
    cli,
    main,
    parse_args,
    write_partitioned_layer,
    write_partitioned_parquet_layer,
    write_csv,
    write_json,
)


def read_rows(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_cli_uses_default_config_path_when_not_overridden():
    args = parse_args([])

    assert args.config == DEFAULT_CONFIG_PATH


def test_cli_accepts_config_path_override(tmp_path, monkeypatch):
    config_path = tmp_path / "pipeline.json"
    called_with = []

    def fake_main(path):
        called_with.append(path)

    monkeypatch.setattr("src.pipeline.main", fake_main)

    cli(["--config", str(config_path)])

    assert called_with == [config_path]


def test_csv_writer_preserves_existing_artifact_when_write_fails(
    tmp_path,
    monkeypatch,
):
    output_path = tmp_path / "orders.csv"
    output_path.write_text("order_id\nexisting\n", encoding="utf-8")

    def fail_writerows(self, rows):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(csv.DictWriter, "writerows", fail_writerows)

    with pytest.raises(OSError, match="simulated disk failure"):
        write_csv(output_path, [{"order_id": "1001"}], ["order_id"])

    assert output_path.read_text(encoding="utf-8") == "order_id\nexisting\n"
    assert list(tmp_path.glob(".orders.csv.*.tmp")) == []


def test_json_writer_preserves_existing_artifact_when_serialization_fails(tmp_path):
    output_path = tmp_path / "pipeline_manifest.json"
    output_path.write_text('{"status": "previous"}\n', encoding="utf-8")

    with pytest.raises(TypeError):
        write_json(output_path, {"bad_value": object()})

    assert output_path.read_text(encoding="utf-8") == '{"status": "previous"}\n'
    assert list(tmp_path.glob(".pipeline_manifest.json.*.tmp")) == []


def test_partitioned_csv_writer_preserves_existing_directory_when_write_fails(
    tmp_path,
    monkeypatch,
):
    base_dir = tmp_path / "silver_orders_by_date"
    existing_partition = base_dir / "order_date=2026-06-01"
    existing_partition.mkdir(parents=True)
    existing_file = existing_partition / "silver_orders.csv"
    existing_file.write_text(
        "order_id,order_date\nexisting,2026-06-01\n",
        encoding="utf-8",
    )

    def fail_writerows(self, rows):
        raise OSError("simulated partition failure")

    monkeypatch.setattr(csv.DictWriter, "writerows", fail_writerows)

    with pytest.raises(OSError, match="simulated partition failure"):
        write_partitioned_layer(
            base_dir,
            [{"order_id": "1001", "order_date": "2026-06-02"}],
            ["order_id", "order_date"],
            "order_date",
            "silver_orders.csv",
        )

    assert existing_file.read_text(encoding="utf-8") == (
        "order_id,order_date\nexisting,2026-06-01\n"
    )
    assert list(tmp_path.glob(".silver_orders_by_date.staged.*")) == []


def test_partitioned_parquet_writer_preserves_existing_directory_when_write_fails(
    tmp_path,
    monkeypatch,
):
    base_dir = tmp_path / "silver_orders_by_date_parquet"
    existing_partition = base_dir / "order_date=2026-06-01"
    existing_partition.mkdir(parents=True)
    existing_file = existing_partition / "silver_orders.parquet"
    existing_file.write_bytes(b"previous parquet bytes")

    def fail_write_table(table, path):
        raise OSError("simulated parquet failure")

    monkeypatch.setattr(pq, "write_table", fail_write_table)

    with pytest.raises(OSError, match="simulated parquet failure"):
        write_partitioned_parquet_layer(
            base_dir,
            [
                {
                    "order_id": "1001",
                    "order_date": "2026-06-02",
                    "quantity": 2,
                }
            ],
            [("order_id", "string"), ("quantity", "int64")],
            "order_date",
            "silver_orders.parquet",
        )

    assert existing_file.read_bytes() == b"previous parquet bytes"
    assert list(tmp_path.glob(".silver_orders_by_date_parquet.staged.*")) == []


def test_pipeline_writes_expected_lakehouse_layers(tmp_path):
    raw_path = tmp_path / "raw" / "orders.csv"
    processed_dir = tmp_path / "processed"
    config_path = tmp_path / "pipeline.json"
    raw_path.parent.mkdir()
    raw_path.write_text(
        "order_id,customer_id,order_date,category,product,quantity,unit_price,status\n"
        "1001,C001,2026-06-01,Electronics,Keyboard,2,1500,delivered\n"
        "1002,C002,2026-06-01,Electronics,Mouse,1,800,cancelled\n"
        "1003,C003,2026-06-02,Home,Chair,2,2500,delivered\n",
        encoding="utf-8",
    )
    config_path.write_text(
        json.dumps(
            {
                "raw_path": str(raw_path),
                "processed_dir": str(processed_dir),
                "included_statuses": ["delivered"],
            }
        ),
        encoding="utf-8",
    )

    main(config_path)

    quality_report = json.loads(
        (processed_dir / "data_quality_report.json").read_text(encoding="utf-8")
    )
    assert quality_report["success"] is True
    assert quality_report["row_count"] == 3
    assert [item["expectation"] for item in quality_report["expectations"]] == [
        "dataset_is_not_empty",
        "required_columns_are_present",
        "rows_are_well_formed",
        "order_ids_are_populated",
        "order_id_is_unique",
        "amounts_are_positive_numbers",
        "order_dates_are_iso_dates",
        "business_dimensions_are_populated",
        "included_statuses_match_source_rows",
    ]

    manifest = json.loads(
        (processed_dir / "pipeline_manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["generated_at_utc"].endswith("Z")
    assert manifest["run"]["config_path"] == str(config_path.resolve())
    assert manifest["run"]["raw_path"] == str(raw_path)
    assert manifest["run"]["processed_dir"] == str(processed_dir)
    assert manifest["run"]["started_at_utc"].endswith("Z")
    assert manifest["run"]["completed_at_utc"].endswith("Z")
    assert manifest["run"]["duration_ms"] >= 0
    assert manifest["source"] == {
        "path": str(raw_path),
        "sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        "rows": 3,
        "profile": {
            "order_date_range": {"min": "2026-06-01", "max": "2026-06-02"},
            "status_counts": {"cancelled": 1, "delivered": 2},
        },
    }
    assert manifest["config"] == {"included_statuses": ["delivered"]}
    assert manifest["layers"] == {
        "bronze": {"rows": 3},
        "rejected": {
            "rows": 1,
            "reasons": {"status_not_included": 1},
        },
        "silver": {
            "rows": 2,
            "profile": {
                "order_date_range": {"min": "2026-06-01", "max": "2026-06-02"},
                "customers": 2,
                "categories": 2,
                "total_revenue": 8000.0,
            },
            "partitions": {
                "field": "order_date",
                "values": ["2026-06-01", "2026-06-02"],
            },
            "parquet_partitions": {
                "field": "order_date",
                "values": ["2026-06-01", "2026-06-02"],
            },
            "partition_inventory": [
                {
                    "value": "2026-06-01",
                    "rows": 1,
                    "csv_path": str(
                        processed_dir
                        / "silver_orders_by_date"
                        / "order_date=2026-06-01"
                        / "silver_orders.csv"
                    ),
                    "parquet_path": str(
                        processed_dir
                        / "silver_orders_by_date_parquet"
                        / "order_date=2026-06-01"
                        / "silver_orders.parquet"
                    ),
                },
                {
                    "value": "2026-06-02",
                    "rows": 1,
                    "csv_path": str(
                        processed_dir
                        / "silver_orders_by_date"
                        / "order_date=2026-06-02"
                        / "silver_orders.csv"
                    ),
                    "parquet_path": str(
                        processed_dir
                        / "silver_orders_by_date_parquet"
                        / "order_date=2026-06-02"
                        / "silver_orders.parquet"
                    ),
                },
            ],
        },
        "gold": {"rows": 2},
        "gold_customer": {"rows": 2},
    }
    assert manifest["quality"] == {"success": True, "expectations": 9}
    assert manifest["artifacts"] == {
        "bronze_orders": str(processed_dir / "bronze_orders.csv"),
        "rejected_orders": str(processed_dir / "rejected_orders.csv"),
        "silver_orders": str(processed_dir / "silver_orders.csv"),
        "silver_orders_by_date": str(processed_dir / "silver_orders_by_date"),
        "silver_orders_by_date_parquet": str(
            processed_dir / "silver_orders_by_date_parquet"
        ),
        "gold_revenue_metrics": str(processed_dir / "gold_revenue_metrics.csv"),
        "gold_customer_metrics": str(processed_dir / "gold_customer_metrics.csv"),
        "data_quality_report": str(processed_dir / "data_quality_report.json"),
    }
    assert set(manifest["artifact_inventory"]) == set(manifest["artifacts"])
    for artifact_name, artifact_stats in manifest["artifact_inventory"].items():
        assert artifact_stats["path"] == manifest["artifacts"][artifact_name]
        assert artifact_stats["exists"] is True
        assert artifact_stats["bytes"] > 0
    assert manifest["artifact_inventory"]["silver_orders"]["type"] == "file"
    assert manifest["artifact_inventory"]["silver_orders"]["files"] == 1
    assert (
        manifest["artifact_inventory"]["silver_orders_by_date"]["type"]
        == "directory"
    )
    assert manifest["artifact_inventory"]["silver_orders_by_date"]["files"] == 2
    assert (
        manifest["artifact_inventory"]["silver_orders_by_date_parquet"]["type"]
        == "directory"
    )
    assert (
        manifest["artifact_inventory"]["silver_orders_by_date_parquet"]["files"]
        == 2
    )

    assert len(read_rows(processed_dir / "bronze_orders.csv")) == 3
    assert read_rows(processed_dir / "rejected_orders.csv") == [
        {
            "order_id": "1002",
            "customer_id": "C002",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Mouse",
            "quantity": "1",
            "unit_price": "800",
            "status": "cancelled",
            "rejection_reason": "status_not_included",
        }
    ]
    assert read_rows(processed_dir / "silver_orders.csv") == [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500.0",
            "revenue": "3000.0",
        },
        {
            "order_id": "1003",
            "customer_id": "C003",
            "order_date": "2026-06-02",
            "category": "Home",
            "product": "Chair",
            "quantity": "2",
            "unit_price": "2500.0",
            "revenue": "5000.0",
        },
    ]
    assert read_rows(
        processed_dir
        / "silver_orders_by_date"
        / "order_date=2026-06-01"
        / "silver_orders.csv"
    ) == [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": "2",
            "unit_price": "1500.0",
            "revenue": "3000.0",
        }
    ]
    assert read_rows(
        processed_dir
        / "silver_orders_by_date"
        / "order_date=2026-06-02"
        / "silver_orders.csv"
    ) == [
        {
            "order_id": "1003",
            "customer_id": "C003",
            "order_date": "2026-06-02",
            "category": "Home",
            "product": "Chair",
            "quantity": "2",
            "unit_price": "2500.0",
            "revenue": "5000.0",
        }
    ]
    parquet_rows = (
        pq.read_table(
            processed_dir
            / "silver_orders_by_date_parquet"
            / "order_date=2026-06-01"
            / "silver_orders.parquet"
        )
        .to_pylist()
    )
    assert parquet_rows == [
        {
            "order_id": "1001",
            "customer_id": "C001",
            "order_date": "2026-06-01",
            "category": "Electronics",
            "product": "Keyboard",
            "quantity": 2,
            "unit_price": 1500.0,
            "revenue": 3000.0,
        }
    ]
    assert read_rows(processed_dir / "gold_revenue_metrics.csv") == [
        {
            "order_date": "2026-06-01",
            "category": "Electronics",
            "orders": "1",
            "units": "2",
            "revenue": "3000.0",
            "average_order_value": "3000.0",
        },
        {
            "order_date": "2026-06-02",
            "category": "Home",
            "orders": "1",
            "units": "2",
            "revenue": "5000.0",
            "average_order_value": "5000.0",
        },
    ]
    assert read_rows(processed_dir / "gold_customer_metrics.csv") == [
        {
            "customer_id": "C003",
            "orders": "1",
            "units": "2",
            "revenue": "5000.0",
            "first_order_date": "2026-06-02",
            "last_order_date": "2026-06-02",
        },
        {
            "customer_id": "C001",
            "orders": "1",
            "units": "2",
            "revenue": "3000.0",
            "first_order_date": "2026-06-01",
            "last_order_date": "2026-06-01",
        },
    ]


def test_pipeline_persists_quality_report_before_failing(tmp_path):
    raw_path = tmp_path / "raw" / "orders.csv"
    processed_dir = tmp_path / "processed"
    config_path = tmp_path / "pipeline.json"
    raw_path.parent.mkdir()
    raw_path.write_text(
        "order_id,customer_id,order_date,category,product,quantity,unit_price,status\n"
        "1001,C001,2026-06-01,Electronics,Keyboard,not-a-number,1500,delivered\n",
        encoding="utf-8",
    )
    config_path.write_text(
        json.dumps(
            {
                "raw_path": str(raw_path),
                "processed_dir": str(processed_dir),
                "included_statuses": ["delivered"],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="amounts_are_positive_numbers"):
        main(config_path)

    quality_report = json.loads(
        (processed_dir / "data_quality_report.json").read_text(encoding="utf-8")
    )
    results = {item["expectation"]: item for item in quality_report["expectations"]}
    assert quality_report["success"] is False
    assert results["amounts_are_positive_numbers"]["observed"] == {
        "invalid_order_ids": ["1001"]
    }
    assert not (processed_dir / "bronze_orders.csv").exists()


def test_pipeline_fails_when_configured_statuses_match_no_source_rows(tmp_path):
    raw_path = tmp_path / "raw" / "orders.csv"
    processed_dir = tmp_path / "processed"
    config_path = tmp_path / "pipeline.json"
    raw_path.parent.mkdir()
    raw_path.write_text(
        "order_id,customer_id,order_date,category,product,quantity,unit_price,status\n"
        "1001,C001,2026-06-01,Electronics,Keyboard,2,1500,cancelled\n",
        encoding="utf-8",
    )
    config_path.write_text(
        json.dumps(
            {
                "raw_path": str(raw_path),
                "processed_dir": str(processed_dir),
                "included_statuses": ["delivered"],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="included_statuses_match_source_rows"):
        main(config_path)

    quality_report = json.loads(
        (processed_dir / "data_quality_report.json").read_text(encoding="utf-8")
    )
    results = {item["expectation"]: item for item in quality_report["expectations"]}
    assert quality_report["success"] is False
    assert results["included_statuses_match_source_rows"]["observed"] == {
        "included_statuses": ["delivered"],
        "matching_rows": 0,
        "matching_status_counts": {},
    }
    assert not (processed_dir / "bronze_orders.csv").exists()
    assert not (processed_dir / "silver_orders.csv").exists()
