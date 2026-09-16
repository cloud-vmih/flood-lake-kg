from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).parents[2]
PACKAGE = ROOT / "src" / "flashflood_data"
LEGACY_MODULES = {
    "flashflood_data.aoi",
    "flashflood_data.budget",
    "flashflood_data.composition",
    "flashflood_data.config",
    "flashflood_data.derive",
    "flashflood_data.harmonize",
    "flashflood_data.http",
    "flashflood_data.inventory",
    "flashflood_data.io_atomic",
    "flashflood_data.models",
    "flashflood_data.paths",
    "flashflood_data.pipeline",
    "flashflood_data.qa",
    "flashflood_data.raster",
    "flashflood_data.registry",
    "flashflood_data.sources",
    "flashflood_data.vector",
}


def imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_package_does_not_import_host_tools() -> None:
    offenders = {
        str(path.relative_to(ROOT)): sorted(
            name
            for name in imports(path)
            if name == "tools" or name.startswith("tools.")
        )
        for path in PACKAGE.rglob("*.py")
        if any(
            name == "tools" or name.startswith("tools.") for name in imports(path)
        )
    }
    assert offenders == {}


def test_static_domain_does_not_import_cli_or_top_level_orchestration() -> None:
    static_dir = PACKAGE / "static"
    forbidden = ("flashflood_data.cli", "flashflood_data.orchestration")
    offenders = {
        str(path.relative_to(ROOT)): sorted(
            name for name in imports(path) if name.startswith(forbidden)
        )
        for path in static_dir.rglob("*.py")
        if any(name.startswith(forbidden) for name in imports(path))
    }
    assert offenders == {}


def test_repository_uses_canonical_module_paths() -> None:
    roots = (ROOT / "src", ROOT / "tests", ROOT / "airflow", ROOT / "spark")
    offenders: dict[str, list[str]] = {}
    for base in roots:
        for path in base.rglob("*.py"):
            used = sorted(
                name
                for name in imports(path)
                if any(
                    name == legacy or name.startswith(f"{legacy}.")
                    for legacy in LEGACY_MODULES
                )
            )
            if used:
                offenders[str(path.relative_to(ROOT))] = used
    assert offenders == {}
