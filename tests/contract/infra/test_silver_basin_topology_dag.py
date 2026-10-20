import ast
from pathlib import Path

ROOT = Path(__file__).parents[3]


def test_silver_basin_dag_is_manual_thin_and_single_writer() -> None:
    source = (
        ROOT / "src/flashflood_data/pipelines/silver_basin/airflow.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert 'dag_id="silver_basin_topology"' in source
    assert "schedule=None" in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert "is_paused_upon_creation=True" in source
    assert "plan_basin_build" in source
    assert "execute_basin_plan" in source
    assert 'pool="silver_writer"' in source
    assert "AirflowException" in source
    assert 'result.get("status") not in {"succeeded", "skipped"}' in source
    imported_roots = {
        node.module.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not {"pyspark", "pyiceberg", "pyarrow", "geopandas"} & imported_roots


def test_airflow_entrypoint_only_exports_domain_dag() -> None:
    source = (ROOT / "airflow/dags/silver_basin_topology.py").read_text(
        encoding="utf-8"
    )
    ast.parse(source)
    assert "flashflood_data.pipelines.silver_basin.airflow" in source
    assert "spark_job" not in source
