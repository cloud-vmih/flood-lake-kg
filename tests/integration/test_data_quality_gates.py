"""Fatal fixture defects remain visible through published QA artifacts."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from flashflood_data.cli import app
from tests.fixtures.static_pipeline.build_fixture_lake import (
    build_fixture_lake,
    isolate_external_boundaries,
)


def test_broken_fixture_fails_qa_after_publishing_report(tmp_path: Path, monkeypatch) -> None:
    """Raising before publication would erase the evidence needed to diagnose a fatal gate."""
    isolate_external_boundaries(monkeypatch)
    paths = build_fixture_lake(tmp_path, broken=True)

    result = CliRunner().invoke(
        app, ["run-static", "--profile", "smoke", "--root", str(tmp_path), "--json-summary"]
    )

    assert result.exit_code == 1
    report = json.loads((paths.qa / "report.json").read_text(encoding="utf-8"))
    event_gate = next(
        check for check in report["checks"] if check["check_id"] == "events.historical.matching"
    )
    assert event_gate["severity"] == "fatal"
    assert not event_gate["passed"]
