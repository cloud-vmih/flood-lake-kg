import ast
from pathlib import Path

ROOT = Path(__file__).parents[3]
DAGS = {
    "gsmap_now_ingest.py": ("gsmap_now_ingest", "gsmap_now.yaml"),
    "gsmap_standard_ingest.py": ("gsmap_standard_ingest", "gsmap_standard.yaml"),
    "era5_land_ingest.py": ("era5_land_ingest", "era5_land.yaml"),
    "ifs_ingest.py": ("ifs_ingest", "ifs.yaml"),
}


def test_each_dynamic_source_has_one_thin_restart_safe_dag() -> None:
    for filename, (dag_id, config_name) in DAGS.items():
        source = (ROOT / "airflow/dags" / filename).read_text(encoding="utf-8")
        ast.parse(source)
        assert dag_id in source
        assert config_name in source
        assert "build_weather_dag" in source
        assert "static_source" not in source


def test_shared_factory_exposes_landing_raw_and_bronze_boundaries() -> None:
    source = (
        ROOT / "src/flashflood_data/orchestration/weather/airflow_factory.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for task_name in (
        "load_cursor",
        "determine_available_end",
        "plan_expected_windows",
        "extract_missing_batches",
        "ensure_source_grid",
        "fetch_missing_or_revised",
        "commit_fetch_attempts",
        "scope_fetched_payload",
        "register_raw_and_meta",
        "verify_contiguous_coverage",
        "advance_cursor",
        "discover_unparsed_batches",
        "parse_bronze",
        "finalize_weather_run",
        "expire_transient_raw",
        "cleanup_weather_staging",
    ):
        assert task_name in source
    assert "landing_raw" in source
    assert "bronze" in source
    assert "catchup=False" in source
    assert "max_active_runs=1" in source
    assert "is_paused_upon_creation=True" in source
    assert ".expand(" in source
    assert "bronze_task_batch_size" in source
    assert '.expand(planned_document=plan["missing"])' not in source
    assert '@task(retries=0, trigger_rule="none_failed")\n    def verify_contiguous_coverage' in source
    assert '@task(retries=0, outlets=[WEATHER_BRONZE_ASSET])' in source
    assert "weather_bronze_updated" in source
    assert "outlets=" in source
    assert 'pool="weather_fetch"' in source
    assert 'pool="weather_raw_writer"' in source
    assert 'trigger_rule="all_done"' in source
    assert "finalize_weather_run(bronze_result, raw_cleanup, staging_cleanup)" in source
    assert "retry_exponential_backoff=True" in source
    assert source.index("ensure_source_grid") < source.index("scope_fetched_payload")
    assert source.index("scope_fetched_payload") < source.index("register_raw_and_meta")
    assert 'trigger_rule="all_done"' in source
    imports = {
        node.module.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not {"xarray", "geopandas", "pyiceberg", "pyarrow"} & imports


def test_dynamic_writers_have_dedicated_serial_pools() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    assert 'airflow pools set weather_raw_writer 1' in compose
    assert 'airflow pools set weather_bronze_writer 1' in compose
    assert 'airflow pools set weather_fetch 4' in compose
