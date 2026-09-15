from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).parents[2]
PACKAGE = ROOT / "src" / "flashflood_data"


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
