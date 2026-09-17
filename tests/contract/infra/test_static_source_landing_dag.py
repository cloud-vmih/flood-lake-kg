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
        "hydrobasins_v1c",
        "basinatlas_v10",
        "hydrorivers_v10",
        "cop_dem_glo30_2024_1",
        "soilgrids_2_0",
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
