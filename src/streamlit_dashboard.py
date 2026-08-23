from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from .pipeline import DEFAULT_CONFIG_PATH, load_config, resolve_pipeline_path
except ImportError:  # Support `streamlit run src/streamlit_dashboard.py`.
    from pipeline import DEFAULT_CONFIG_PATH, load_config, resolve_pipeline_path


DEFAULT_MANIFEST_NAME = "pipeline_manifest.json"


def default_manifest_path(config_path=DEFAULT_CONFIG_PATH):
    config = load_config(config_path)
    return resolve_pipeline_path(config["processed_dir"]) / DEFAULT_MANIFEST_NAME


def load_manifest(path):
    path = Path(path)
    with path.open(encoding="utf-8") as file:
        manifest = json.load(file)

    if not isinstance(manifest, dict):
        raise ValueError(f"Manifest must contain a JSON object: {path}")
    return manifest


def format_percent(value):
    if value is None:
        return "n/a"
    return f"{float(value) * 100:.1f}%"


def build_kpi_cards(manifest):
    business_impact = manifest.get("business_impact", {})
    if not isinstance(business_impact, dict):
        business_impact = {}
    orders = business_impact.get("orders", {})
    if not isinstance(orders, dict):
        orders = {}
    revenue = business_impact.get("revenue", {})
    if not isinstance(revenue, dict):
        revenue = {}
    health = manifest.get("health", {})
    quality = manifest.get("quality", {})

    return [
        {
            "label": "Health",
            "value": health.get("status", "unknown"),
            "help": f"{health.get('warning_count', 0)} warning(s)",
        },
        {
            "label": "Quality",
            "value": "passed" if quality.get("success") else "failed",
            "help": ", ".join(
                quality.get("summary", {}).get("failed_expectations", [])
            ) or "No failed expectations",
        },
        {
            "label": "Accepted orders",
            "value": orders.get("accepted", 0),
            "help": f"Rejected {orders.get('rejected', 0)}",
        },
        {
            "label": "Rejection rate",
            "value": format_percent(orders.get("rejection_rate")),
            "help": "Rejected orders divided by source orders",
        },
        {
            "label": "Accepted revenue",
            "value": revenue.get("accepted", 0.0),
            "help": f"Realized {format_percent(revenue.get('realized_rate'))}",
        },
        {
            "label": "Rejected potential revenue",
            "value": revenue.get("rejected_potential", 0.0),
            "help": "Revenue excluded by configured load scope",
        },
    ]


def build_layer_rows(manifest):
    layers = manifest.get("layers", {})
    rows = []
    for layer_name in [
        "bronze",
        "silver",
        "rejected",
        "gold",
        "gold_customer",
        "gold_category",
        "gold_rejection",
    ]:
        layer = layers.get(layer_name, {})
        rows.append(
            {
                "layer": layer_name,
                "rows": layer.get("rows", 0),
            }
        )
    return rows


def build_rejection_reason_rows(manifest):
    reasons = (
        manifest.get("layers", {})
        .get("rejected", {})
        .get("reasons", {})
    )
    return [
        {"rejection_reason": reason, "rows": count}
        for reason, count in sorted(reasons.items())
    ]


def build_artifact_change_rows(manifest):
    changes = manifest.get("run_comparison", {}).get(
        "artifact_checksum_changes", {}
    )
    return [
        {
            "artifact": artifact,
            "previous_exists": details.get("previous_exists"),
            "current_exists": details.get("current_exists"),
            "checksum_changed": details.get("sha256_changed"),
        }
        for artifact, details in sorted(changes.items())
        if isinstance(details, dict)
    ]


def build_business_impact_delta_rows(manifest):
    deltas = manifest.get("run_comparison", {}).get("business_impact_deltas", {})
    if not isinstance(deltas, dict):
        return []
    metric_groups = [
        ("orders", "accepted", "Accepted orders"),
        ("orders", "rejected", "Rejected orders"),
        ("orders", "rejection_rate", "Rejection rate"),
        ("revenue", "accepted", "Accepted revenue"),
        ("revenue", "rejected_potential", "Rejected potential revenue"),
        ("revenue", "realized_rate", "Realized revenue rate"),
    ]
    rows = []
    for group_name, metric_name, label in metric_groups:
        group = deltas.get(group_name, {})
        if not isinstance(group, dict):
            continue
        details = group.get(metric_name, {})
        if not isinstance(details, dict) or not details:
            continue
        rows.append(
            {
                "metric": label,
                "previous": details.get("previous"),
                "current": details.get("current"),
                "delta": details.get("delta"),
            }
        )
    return rows


def render_dashboard(manifest, streamlit_module):
    st = streamlit_module
    run = manifest.get("run", {})
    source = manifest.get("source", {})
    config = manifest.get("config", {})
    health = manifest.get("health", {})

    st.set_page_config(page_title="Retail Lakehouse Dashboard", layout="wide")
    st.title("Retail Lakehouse Dashboard")
    st.caption(f"Completed at {run.get('completed_at_utc', 'unknown')}")

    metric_columns = st.columns(3)
    for index, card in enumerate(build_kpi_cards(manifest)):
        with metric_columns[index % 3]:
            st.metric(card["label"], card["value"], help=card["help"])

    st.subheader("Run Scope")
    st.write(
        {
            "source": source.get("path"),
            "included_statuses": config.get("included_statuses", []),
            "order_date_window": config.get("order_date_window", {}),
        }
    )

    business_impact_delta_rows = build_business_impact_delta_rows(manifest)
    if business_impact_delta_rows:
        st.subheader("Business Impact Delta")
        st.dataframe(
            business_impact_delta_rows,
            hide_index=True,
            use_container_width=True,
        )

    st.subheader("Layer Row Counts")
    st.bar_chart(build_layer_rows(manifest), x="layer", y="rows")
    st.dataframe(build_layer_rows(manifest), hide_index=True, use_container_width=True)

    rejection_rows = build_rejection_reason_rows(manifest)
    if rejection_rows:
        st.subheader("Rejection Reasons")
        st.bar_chart(rejection_rows, x="rejection_reason", y="rows")
        st.dataframe(rejection_rows, hide_index=True, use_container_width=True)

    if health.get("threshold_breaches"):
        st.subheader("Health Threshold Breaches")
        st.dataframe(
            health["threshold_breaches"],
            hide_index=True,
            use_container_width=True,
        )

    artifact_changes = build_artifact_change_rows(manifest)
    if artifact_changes:
        st.subheader("Changed Artifacts")
        st.dataframe(artifact_changes, hide_index=True, use_container_width=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the optional Streamlit manifest dashboard."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help=(
            "Path to pipeline_manifest.json. Defaults to the processed_dir "
            "from config/pipeline.json."
        ),
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    manifest_path = args.manifest or default_manifest_path()
    manifest = load_manifest(manifest_path)

    try:
        import streamlit as st
    except ImportError as exc:
        raise SystemExit(
            "Streamlit is not installed. Install it with "
            "`python -m pip install streamlit`, then run "
            "`streamlit run src/streamlit_dashboard.py`."
        ) from exc

    render_dashboard(manifest, st)


if __name__ == "__main__":
    main()
