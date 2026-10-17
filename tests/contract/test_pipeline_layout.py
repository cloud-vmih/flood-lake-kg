from __future__ import annotations

import ast
import importlib
from pathlib import Path

ROOT = Path(__file__).parents[2]
SOURCE_ROOTS = (ROOT / "src", ROOT / "airflow", ROOT / "tests")
DAG_IMPORTS = {
    "bronze_quality_backfill.py": "flashflood_data.pipelines.quality_migration.airflow",
    "static_source_landing.py": "flashflood_data.pipelines.static_landing.airflow",
    "static_source_to_bronze.py": "flashflood_data.pipelines.static_bronze.airflow",
    "era5_land_ingest.py": "flashflood_data.pipelines.weather_ingest.airflow",
    "gsmap_now_ingest.py": "flashflood_data.pipelines.weather_ingest.airflow",
    "gsmap_standard_ingest.py": "flashflood_data.pipelines.weather_ingest.airflow",
    "ifs_ingest.py": "flashflood_data.pipelines.weather_ingest.airflow",
}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_pipeline_public_modules_are_importable() -> None:
    modules = (
        "flashflood_data.pipelines.study_area.pipeline",
        "flashflood_data.pipelines.static_landing.runtime",
        "flashflood_data.pipelines.static_bronze.runtime",
        "flashflood_data.pipelines.silver_static_features.terrain",
        "flashflood_data.pipelines.weather_ingest.runtime",
        "flashflood_data.shared.contracts.source_objects",
        "flashflood_data.shared.geospatial.raster",
        "flashflood_data.shared.geospatial.vector",
        "flashflood_data.shared.metadata.factory",
    )
    for module in modules:
        assert importlib.import_module(module)


def test_airflow_dag_files_are_thin_pipeline_entrypoints() -> None:
    for filename, expected_import in DAG_IMPORTS.items():
        path = ROOT / "airflow" / "dags" / filename
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert len(lines) <= 12, filename
        assert _imports(path) == {expected_import}


def test_airflow_entrypoints_pass_safe_mode_discovery_heuristic() -> None:
    for filename in DAG_IMPORTS:
        content = (ROOT / "airflow" / "dags" / filename).read_bytes().lower()
        assert b"airflow" in content, filename
        assert b"dag" in content, filename


def test_active_python_does_not_import_legacy_orchestration_namespace() -> None:
    offenders: dict[str, list[str]] = {}
    for source_root in SOURCE_ROOTS:
        for path in source_root.rglob("*.py"):
            if path == Path(__file__):
                continue
            legacy = sorted(
                name
                for name in _imports(path)
                if name == "flashflood_data.orchestration"
                or name.startswith("flashflood_data.orchestration.")
            )
            if legacy:
                offenders[str(path.relative_to(ROOT))] = legacy
    assert offenders == {}


def test_legacy_static_package_is_removed() -> None:
    assert not (ROOT / "src" / "flashflood_data" / "static").exists()
    offenders: dict[str, list[str]] = {}
    for source_root in SOURCE_ROOTS:
        for path in source_root.rglob("*.py"):
            if path == Path(__file__):
                continue
            legacy = sorted(
                name
                for name in _imports(path)
                if name == "flashflood_data.static"
                or name.startswith("flashflood_data.static.")
            )
            if legacy:
                offenders[str(path.relative_to(ROOT))] = legacy
    assert offenders == {}


def test_storage_does_not_depend_on_pipeline_packages() -> None:
    storage = ROOT / "src" / "flashflood_data" / "storage"
    offenders = {
        str(path.relative_to(ROOT)): sorted(
            name
            for name in _imports(path)
            if name == "flashflood_data.pipelines"
            or name.startswith("flashflood_data.pipelines.")
        )
        for path in storage.rglob("*.py")
        if any(
            name == "flashflood_data.pipelines"
            or name.startswith("flashflood_data.pipelines.")
            for name in _imports(path)
        )
    }
    assert offenders == {}


def test_aoi_uses_dedicated_cli_instead_of_file_first_workflow() -> None:
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    cli = (ROOT / "src" / "flashflood_data" / "cli" / "app.py").read_text(
        encoding="utf-8"
    )
    assert "flashflood-data build-aoi --root ." in makefile
    assert 'name="build-aoi"' in cli
    assert "run-static" not in makefile
