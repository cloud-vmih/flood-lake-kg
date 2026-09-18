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
        "historical_flood_evidence_2020_2026",
        "geofabrik_vietnam_snapshot",
        "cop_dem_glo30_2024_1",
        "soilgrids_2_0",
        "esa_worldcover_2021_v200",
    } <= _string_literals(tree)
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
    assert "cleanup_committed_staging(" in cleanup_text
    assert "build_static_landing_service" not in cleanup_text
    assert "StaticSourceLandingService._error_code(error)" in text
    assert "source_ids: tuple[str, ...]" in text
    assert '"upstream_task_failed"' in text
