import importlib
import json
from pathlib import Path
from typing import ClassVar

import pytest
from typer.testing import CliRunner

from flashflood_data.cli import app
from flashflood_data.static.sources.base import SourceConfigurationError
from flashflood_data.static.sources.cop_dem import MissingCredentials
from flashflood_data.static.workflow import RunSummary, Stage
from flashflood_data.storage.http import BudgetRejected

CLI_MODULE = importlib.import_module("flashflood_data.cli.app")


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
        "land-static",
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


class RecordingLandingService:
    calls: ClassVar[list[tuple[list[str], str | None]]] = []

    def run(self, source_ids, run_id=None):
        from flashflood_data.orchestration.landing.models import LandingRunSummary

        self.calls.append((list(source_ids), run_id))
        return LandingRunSummary(
            run_id=run_id or "generated",
            status="completed",
            completed_sources=tuple(source_ids),
        )


def test_landing_service_uses_project_root_environment(
    monkeypatch, tmp_path: Path
) -> None:
    class StopAfterPathCheck(RuntimeError):
        pass

    def check_study_area_path(path: Path):
        assert path == tmp_path.resolve() / "config" / "study_area.yaml"
        raise StopAfterPathCheck

    monkeypatch.setenv("FLASHFLOOD_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setattr(CLI_MODULE, "load_study_area", check_study_area_path)

    with pytest.raises(StopAfterPathCheck):
        CLI_MODULE.build_static_landing_service()


def test_land_static_routes_sources_and_run_id(monkeypatch, tmp_path: Path) -> None:
    RecordingLandingService.calls = []
    monkeypatch.setattr(
        CLI_MODULE,
        "build_static_landing_service",
        lambda root=None: RecordingLandingService(),
    )
    result = CliRunner().invoke(
        app,
        [
            "land-static",
            "--root",
            str(tmp_path),
            "--source",
            "hydrobasins_v1c",
            "--run-id",
            "manual-1",
            "--json-summary",
        ],
    )

    assert result.exit_code == 0, result.output
    assert RecordingLandingService.calls == [(["hydrobasins_v1c"], "manual-1")]
    assert json.loads(result.stdout)["status"] == "completed"


def test_land_static_uses_all_configured_sources_by_default(monkeypatch, tmp_path: Path) -> None:
    RecordingLandingService.calls = []
    monkeypatch.setattr(
        CLI_MODULE,
        "build_static_landing_service",
        lambda root=None: RecordingLandingService(),
    )

    result = CliRunner().invoke(app, ["land-static", "--root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert RecordingLandingService.calls == [
        (
            [
                "hydrobasins_v1c",
                "basinatlas_v10",
                "hydrorivers_v10",
                "cop_dem_glo30_2024_1",
                "soilgrids_2_0",
            ],
            None,
        )
    ]


def test_land_static_returns_nonzero_for_incomplete_run(monkeypatch, tmp_path: Path) -> None:
    from flashflood_data.orchestration.landing.models import LandingRunSummary

    class IncompleteService:
        def run(self, source_ids, run_id=None):
            return LandingRunSummary(
                run_id=run_id or "generated",
                status="partial_failure",
                completed_sources=(source_ids[0],),
                failed_sources=(source_ids[-1],),
            )

    monkeypatch.setattr(
        CLI_MODULE, "build_static_landing_service", lambda root=None: IncompleteService()
    )

    result = CliRunner().invoke(
        app,
        [
            "land-static",
            "--root",
            str(tmp_path),
            "--source",
            "hydrobasins_v1c",
            "--source",
            "basinatlas_v10",
        ],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["status"] == "partial_failure"
