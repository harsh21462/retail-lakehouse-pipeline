import importlib.util
import sys
from pathlib import Path, PurePosixPath


def load_dag_module():
    module_path = Path(__file__).resolve().parents[1] / "dags" / "retail_lakehouse_dag.py"
    spec = importlib.util.spec_from_file_location("retail_lakehouse_dag", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_airflow_dag_module_imports_with_or_without_airflow_installed():
    module = load_dag_module()

    if module.DAG is None:
        assert module.create_dag() is None
        assert module.dag is None
    else:
        assert module.dag.dag_id == "retail_lakehouse_daily"


def test_pipeline_command_uses_cli_entrypoint_and_quotes_paths():
    module = load_dag_module()

    command = module.build_pipeline_command(
        project_root=PurePosixPath("/opt/retail lakehouse"),
        config_path=PurePosixPath("/opt/retail lakehouse/config/prod pipeline.json"),
        python_bin="/venv/bin/python",
    )

    assert command == (
        "cd '/opt/retail lakehouse' && /venv/bin/python src/pipeline.py "
        "--config '/opt/retail lakehouse/config/prod pipeline.json'"
    )
