"""The production parse pipeline must run as a distinct Airflow DAG."""

import ast
from pathlib import Path

ROOT = Path(__file__).parents[3]
DAG = ROOT / "airflow" / "dags" / "static_source_to_bronze.py"


def test_bronze_dag_maps_registered_raw_ids_to_parse_tasks() -> None:
    source = DAG.read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert "static_source_to_bronze" in source
    assert "schedule=None" in source
    assert "max_active_runs=1" in source
    assert "discover_objects" in source
    assert "process_object" in source
    assert "load_bronze_config" in source
    assert "requested_source_id" in source
    assert "dag_run.conf.get('source_id'" in source
    assert "dag_run.conf.get('force_reprocess'" in source
    assert ".expand(" in source
    assert "source_landing_writer" not in source
    assert "bronze_writer" in source
    imports = {
        node.module.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not {"rasterio", "geopandas", "pyiceberg", "pyarrow"} & imports


def test_bronze_dag_does_not_fetch_provider_data() -> None:
    source = DAG.read_text(encoding="utf-8")
    assert "build_bronze_service" in source
    assert "acquire_validated_assets" not in source
    assert "publish_file" not in source


def test_airflow_bootstrap_creates_bronze_writer_pool() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    assert "airflow pools set bronze_writer 1" in compose


def test_airflow_components_share_api_and_log_server_signing_keys() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    assert "AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_API_SECRET_KEY:" in compose
    assert "AIRFLOW__API__SECRET_KEY: ${AIRFLOW_API_SECRET_KEY:" in compose
