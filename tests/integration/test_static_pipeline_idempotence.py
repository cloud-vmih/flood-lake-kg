"""An unchanged static lake is a checksum-stable no-op after its first build."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from typer.testing import CliRunner

from flashflood_data.cli import app
from tests.fixtures.static_pipeline.build_fixture_lake import (
    build_fixture_lake,
    isolate_external_boundaries,
)


def _output_checksums(root: Path) -> dict[str, str]:
    dataset = root / "dataset"
    return {
        path.relative_to(dataset).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for output_root in (dataset / "derived", dataset / "qa")
        for path in sorted(output_root.rglob("*"))
        if path.is_file()
    }


def test_static_pipeline_second_run_is_noop(tmp_path: Path, monkeypatch) -> None:
    """Rebuilding any unchanged final product would change its run metadata or work counters."""
    isolate_external_boundaries(monkeypatch)
    build_fixture_lake(tmp_path)
    runner = CliRunner()
    first = runner.invoke(app, ["run-static", "--profile", "smoke", "--root", str(tmp_path)])
    assert first.exit_code == 0, first.output
    before = _output_checksums(tmp_path)

    second = runner.invoke(
        app, ["run-static", "--profile", "smoke", "--root", str(tmp_path), "--json-summary"]
    )

    assert second.exit_code == 0, second.output
    summary = json.loads(second.stdout)
    assert summary["fetched"] == 0
    assert summary["harmonized"] == 0
    assert summary["derived"] == 0
    assert before == _output_checksums(tmp_path)
