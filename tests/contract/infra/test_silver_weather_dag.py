"""Silver weather Airflow entrypoint stays thin and paused until explicitly run."""

import ast
from pathlib import Path

ROOT = Path(__file__).parents[3]


def test_silver_weather_dag_is_manual_and_uses_one_spark_writer_task() -> None:
    path = ROOT / "src/flashflood_data/pipelines/silver_weather/airflow.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert 'dag_id="silver_weather_aggregation"' in source
    assert "schedule=None" in source
    assert "max_active_runs=1" in source
    assert "is_paused_upon_creation=True" in source
    assert 'pool="silver_writer"' in source
    assert "execute_weather_plan" in source
    imports = {
        node.module.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not {"pyspark", "pyiceberg", "pyarrow"} & imports


def test_dag_entrypoint_exports_only_weather_dag() -> None:
    source = (ROOT / "airflow/dags/silver_weather_aggregation.py").read_text(encoding="utf-8")
    assert "flashflood_data.pipelines.silver_weather.airflow" in source
    assert "spark_job" not in source


def test_dag_entrypoint_passes_airflow_safe_discovery() -> None:
    source = (ROOT / "airflow/dags/silver_weather_aggregation.py").read_bytes().lower()
    assert b"airflow" in source
    assert b"dag" in source
