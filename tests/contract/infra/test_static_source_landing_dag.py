import ast
from pathlib import Path

ROOT = Path(__file__).parents[3]
DAG_PATH = ROOT / "airflow/dags/static_source_landing.py"


def _imported_roots(tree: ast.AST) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots


def _string_literals(tree: ast.AST) -> set[str]:
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def test_static_source_landing_dag_is_thin_and_source_isolated() -> None:
    text = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(text)

    assert "static_source_landing" in text
    assert "source_landing_writer" in text
    assert {
        "sonla_admin_2025",
        "gadm_vnm_4_1",
        "hydrobasins_v1c",
        "basinatlas_v10",
        "hydrorivers_v10",
        "worldpop_vnm_2025",
        "geofabrik_vietnam_snapshot",
        "cop_dem_glo30_2024_1",
        "soilgrids_2_0",
        "esa_worldcover_2021_v200",
    } <= _string_literals(tree)
    assert "historical_flood_evidence_2020_2026" not in _string_literals(tree)
    assert not ({"geopandas", "rasterio", "pyiceberg", "pyarrow"} & _imported_roots(tree))
    assert "harmonize" not in text


def test_dag_uses_manual_paused_runs_envelopes_and_all_done_summary() -> None:
    text = DAG_PATH.read_text(encoding="utf-8")

    assert "schedule=None" in text
    assert "catchup=False" in text
    assert "max_active_runs=1" in text
    assert "is_paused_upon_creation=True" in text
    assert 'trigger_rule="all_done"' in text
    assert "LandingTaskEnvelope" in text
    assert text.count('.override(pool="source_landing_writer")') == 3
    assert "publish_source" in text
    assert "register_batch" in text
    assert "cleanup_batch" in text


def test_taskflow_callables_do_not_use_reserved_context_argument_names() -> None:
    tree = ast.parse(DAG_PATH.read_text(encoding="utf-8"))
    task_functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(
            (isinstance(decorator, ast.Name) and decorator.id == "task")
            or (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "task"
            )
            for decorator in node.decorator_list
        )
    ]

    assert task_functions
    for function in task_functions:
        assert "run_id" not in {argument.arg for argument in function.args.args}


def test_source_groups_release_staging_before_the_next_source_publishes() -> None:
    text = DAG_PATH.read_text(encoding="utf-8")

    assert "previous_cleanup >> published" in text


def test_dag_config_uses_the_configured_project_root() -> None:
    text = DAG_PATH.read_text(encoding="utf-8")

    assert "ProjectPaths.discover().root" in text


def test_existing_landing_dag_seeds_meta_and_audits_registered_raw() -> None:
    text = DAG_PATH.read_text(encoding="utf-8")
    assert "register_meta_registry" in text
    assert "audit_registered_meta" in text
    assert "registry_ready >> published" in text
    assert "registered = register_batch" in text
    assert "audited = audit_registered_meta" in text


def test_failure_envelopes_clean_staging_and_summary_handles_missing_xcoms() -> None:
    text = DAG_PATH.read_text(encoding="utf-8")
    tree = ast.parse(text)
    cleanup = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "cleanup_batch"
    )
    cleanup_text = ast.get_source_segment(text, cleanup)

    assert "cleanup_failed_staging(" in cleanup_text
    assert "build_static_landing_service" in cleanup_text
    assert "service.cleanup_batch(" in cleanup_text
    assert "StaticSourceLandingService._error_code(error)" in text
    assert "source_ids: tuple[str, ...]" in text
    assert '"upstream_task_failed"' in text


def test_source_failures_log_the_pipeline_phase_and_traceback() -> None:
    text = DAG_PATH.read_text(encoding="utf-8")

    assert "def _log_source_failure(" in text
    assert "traceback.format_tb" in text
    assert text.count("_log_source_failure(") >= 5
    assert {"publish", "register", "cleanup", "audit"} <= _string_literals(
        ast.parse(text)
    )
