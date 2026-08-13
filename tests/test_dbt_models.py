from pathlib import Path

from src.pipeline import SQL_MODEL_DEFINITIONS


ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = ROOT / "dbt"


def test_dbt_project_declares_gold_models_for_pipeline_sql_inventory():
    project = (DBT_ROOT / "dbt_project.yml").read_text(encoding="utf-8")
    schema = (DBT_ROOT / "models" / "gold" / "schema.yml").read_text(
        encoding="utf-8"
    )

    assert "name: retail_lakehouse" in project
    for model in SQL_MODEL_DEFINITIONS:
        model_name = model["name"]
        model_sql = DBT_ROOT / "models" / "gold" / f"{model_name}.sql"

        assert model_sql.exists()
        assert f"- name: {model_name}" in schema


def test_dbt_gold_models_keep_expected_output_columns_and_sources():
    expected_sources = {
        "gold_revenue_metrics": "{{ source('lakehouse', 'silver_orders') }}",
        "gold_customer_metrics": "{{ source('lakehouse', 'silver_orders') }}",
        "gold_category_metrics": "{{ source('lakehouse', 'silver_orders') }}",
        "gold_rejection_metrics": "{{ source('lakehouse', 'rejected_orders') }}",
    }

    for model in SQL_MODEL_DEFINITIONS:
        sql = (
            DBT_ROOT / "models" / "gold" / f"{model['name']}.sql"
        ).read_text(encoding="utf-8")

        assert expected_sources[model["name"]] in sql
        for column in model["output_columns"]:
            assert column in sql
