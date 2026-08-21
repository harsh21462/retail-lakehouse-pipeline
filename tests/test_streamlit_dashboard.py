import json

import pytest

from src.streamlit_dashboard import (
    build_artifact_change_rows,
    build_kpi_cards,
    build_layer_rows,
    build_rejection_reason_rows,
    default_manifest_path,
    format_percent,
    load_manifest,
)


def test_default_manifest_path_uses_configured_processed_dir(tmp_path):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps(
            {
                "raw_path": "data/raw/orders.csv",
                "processed_dir": str(tmp_path / "processed"),
                "included_statuses": ["delivered"],
            }
        ),
        encoding="utf-8",
    )

    assert default_manifest_path(config_path) == (
        tmp_path / "processed" / "pipeline_manifest.json"
    )


def test_load_manifest_rejects_non_object_json(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    manifest_path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        load_manifest(manifest_path)


def test_format_percent_handles_missing_values():
    assert format_percent(None) == "n/a"
    assert format_percent(0.125) == "12.5%"


def test_dashboard_helpers_shape_manifest_for_streamlit_widgets():
    manifest = {
        "health": {
            "status": "warning",
            "warning_count": 1,
        },
        "quality": {
            "success": False,
            "summary": {"failed_expectations": ["amounts_are_positive_numbers"]},
        },
        "business_impact": {
            "orders": {
                "accepted": 8,
                "rejected": 2,
                "rejection_rate": 0.2,
            },
            "revenue": {
                "accepted": 1200.0,
                "rejected_potential": 300.0,
                "realized_rate": 0.8,
            },
        },
        "layers": {
            "bronze": {"rows": 10},
            "silver": {"rows": 8},
            "rejected": {
                "rows": 2,
                "reasons": {
                    "order_date_out_of_range": 1,
                    "status_not_included": 1,
                },
            },
            "gold": {"rows": 3},
        },
        "run_comparison": {
            "artifact_checksum_changes": {
                "silver_orders": {
                    "previous_exists": True,
                    "current_exists": True,
                    "sha256_changed": True,
                },
                "malformed": "ignored",
            },
        },
    }

    assert build_kpi_cards(manifest) == [
        {"label": "Health", "value": "warning", "help": "1 warning(s)"},
        {
            "label": "Quality",
            "value": "failed",
            "help": "amounts_are_positive_numbers",
        },
        {"label": "Accepted orders", "value": 8, "help": "Rejected 2"},
        {
            "label": "Rejection rate",
            "value": "20.0%",
            "help": "Rejected orders divided by source orders",
        },
        {"label": "Accepted revenue", "value": 1200.0, "help": "Realized 80.0%"},
        {
            "label": "Rejected potential revenue",
            "value": 300.0,
            "help": "Revenue excluded by configured load scope",
        },
    ]
    assert build_layer_rows(manifest)[:4] == [
        {"layer": "bronze", "rows": 10},
        {"layer": "silver", "rows": 8},
        {"layer": "rejected", "rows": 2},
        {"layer": "gold", "rows": 3},
    ]
    assert build_rejection_reason_rows(manifest) == [
        {"rejection_reason": "order_date_out_of_range", "rows": 1},
        {"rejection_reason": "status_not_included", "rows": 1},
    ]
    assert build_artifact_change_rows(manifest) == [
        {
            "artifact": "silver_orders",
            "previous_exists": True,
            "current_exists": True,
            "checksum_changed": True,
        }
    ]
