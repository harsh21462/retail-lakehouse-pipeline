import csv
import json

from src.pipeline import main


def read_rows(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


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
        "order_id_is_unique",
        "amounts_are_positive_numbers",
    ]

    assert len(read_rows(processed_dir / "bronze_orders.csv")) == 3
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
