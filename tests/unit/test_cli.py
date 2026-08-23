import json
from pathlib import Path
from typing import ClassVar

from typer.testing import CliRunner

from flashflood_data.cli import app
from flashflood_data.pipeline import RunSummary, Stage


def test_cli_lists_static_stages() -> None:
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in (
        "inventory",
        "fetch",
        "validate",
        "harmonize",
        "derive",
        "map",
        "run-static",
        "cleanup",
    ):
        assert command in result.stdout


class RecordingPipeline:
    calls: ClassVar[list[tuple[list[Stage], list[str] | None, bool, str]]] = []

    def __init__(self, paths, *, profile: str) -> None:
        self.paths = paths
        self.profile = profile

    def run(self, stages, source_ids, *, resolve_only: bool, command: str) -> RunSummary:
        self.calls.append((list(stages), source_ids, resolve_only, command))
        return RunSummary(run_id="fixture", status="completed")


def test_fetch_routes_sources_and_resolve_only_to_pipeline(monkeypatch, tmp_path: Path) -> None:
    RecordingPipeline.calls = []
    monkeypatch.setattr("flashflood_data.cli.StaticPipeline", RecordingPipeline)

    result = CliRunner().invoke(
        app,
        [
            "fetch",
            "--root",
            str(tmp_path),
            "--source",
            "first",
            "--source",
            "second",
            "--resolve-only",
            "--json-summary",
        ],
    )

    assert result.exit_code == 0
    assert RecordingPipeline.calls == [([Stage.FETCH], ["first", "second"], True, "fetch")]
    assert json.loads(result.stdout)["status"] == "completed"


def test_run_static_uses_verified_prefix_and_live_profile(monkeypatch, tmp_path: Path) -> None:
    RecordingPipeline.calls = []
    monkeypatch.setattr("flashflood_data.cli.StaticPipeline", RecordingPipeline)

    result = CliRunner().invoke(
        app,
        ["run-static", "--root", str(tmp_path), "--profile", "live", "--stop-after", "aoi"],
    )

    assert result.exit_code == 0
    assert RecordingPipeline.calls == [
        ([Stage.INVENTORY, Stage.BOOTSTRAP_ADMIN, Stage.AOI], None, False, "run-static")
    ]


def test_run_static_rejects_unknown_profile(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app, ["run-static", "--root", str(tmp_path), "--profile", "offline"]
    )

    assert result.exit_code == 2
