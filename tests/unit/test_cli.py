import json
from pathlib import Path
from typing import ClassVar

from typer.testing import CliRunner

from flashflood_data.cli import app
from flashflood_data.http import BudgetRejected
from flashflood_data.pipeline import RunSummary, Stage
from flashflood_data.static.sources.base import SourceConfigurationError
from flashflood_data.static.sources.cop_dem import MissingCredentials


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


def test_map_routes_to_its_own_stage(monkeypatch, tmp_path: Path) -> None:
    RecordingPipeline.calls = []
    monkeypatch.setattr("flashflood_data.cli.StaticPipeline", RecordingPipeline)

    result = CliRunner().invoke(app, ["map", "--root", str(tmp_path)])

    assert result.exit_code == 0
    assert RecordingPipeline.calls[0][0][0].value == "map"


class CredentialsPipeline(RecordingPipeline):
    def run(self, stages, source_ids, *, resolve_only: bool, command: str) -> RunSummary:
        raise MissingCredentials("CDSE credentials missing")


def test_cli_treats_missing_credentials_as_configuration_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("flashflood_data.cli.StaticPipeline", CredentialsPipeline)

    result = CliRunner().invoke(app, ["fetch", "--root", str(tmp_path)])

    assert result.exit_code == 2


class BudgetPipeline(RecordingPipeline):
    def run(self, stages, source_ids, *, resolve_only: bool, command: str) -> RunSummary:
        raise BudgetRejected("minimum_free_space")


def test_cli_treats_budget_rejection_as_configuration_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("flashflood_data.cli.StaticPipeline", BudgetPipeline)

    result = CliRunner().invoke(app, ["fetch", "--root", str(tmp_path)])

    assert result.exit_code == 2


class ConfigurationPipeline(RecordingPipeline):
    def run(self, stages, source_ids, *, resolve_only: bool, command: str) -> RunSummary:
        raise SourceConfigurationError("fixture setting is invalid")


def test_cli_treats_typed_adapter_configuration_as_configuration_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("flashflood_data.cli.StaticPipeline", ConfigurationPipeline)

    result = CliRunner().invoke(app, ["fetch", "--root", str(tmp_path)])

    assert result.exit_code == 2


def test_run_static_rejects_unknown_profile(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app, ["run-static", "--root", str(tmp_path), "--profile", "offline"]
    )

    assert result.exit_code == 2
