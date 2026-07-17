from __future__ import annotations

import os
import shlex
import sys
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(
    os.environ.get("RETAIL_LAKEHOUSE_PROJECT_ROOT", Path(__file__).resolve().parents[1])
).resolve()
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline.json"
DEFAULT_PYTHON_BIN = os.environ.get("RETAIL_LAKEHOUSE_PYTHON_BIN", sys.executable)


def build_pipeline_command(
    *,
    project_root: Path | str = PROJECT_ROOT,
    config_path: Path | str = DEFAULT_CONFIG_PATH,
    python_bin: str = DEFAULT_PYTHON_BIN,
) -> str:
    """Build the scheduled command without duplicating pipeline internals."""
    return " && ".join(
        [
            f"cd {shlex.quote(str(project_root))}",
            (
                f"{shlex.quote(str(python_bin))} src/pipeline.py "
                f"--config {shlex.quote(str(config_path))}"
            ),
        ]
    )


try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ModuleNotFoundError:
    DAG = None
    BashOperator = None


def create_dag():
    if DAG is None or BashOperator is None:
        return None

    with DAG(
        dag_id="retail_lakehouse_daily",
        description="Run the retail lakehouse batch pipeline from raw CSV to gold tables.",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args={
            "owner": "data-engineering",
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["retail", "lakehouse", "portfolio"],
    ) as dag:
        BashOperator(
            task_id="run_retail_lakehouse_pipeline",
            bash_command=build_pipeline_command(),
            env={"PYTHONPATH": str(PROJECT_ROOT)},
        )

    return dag


dag = create_dag()
