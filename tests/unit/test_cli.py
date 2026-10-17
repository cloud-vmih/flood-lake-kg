"""CLI contracts for the supported AOI, landing, and Bronze entry points."""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import pytest
from typer.testing import CliRunner

from flashflood_data.cli import app

CLI_MODULE = importlib.import_module("flashflood_data.cli.app")
LANDING_RUNTIME = importlib.import_module("flashflood_data.pipelines.static_landing.runtime")


def test_cli_lists_only_supported_top_level_commands() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in ("build-aoi", "land-static", "bronze"):
        assert command in result.stdout
    assert "run-static" not in result.stdout


@dataclass(frozen=True)
class _AoiResult:
    run_id: str

    def to_document(self) -> dict[str, str]:
        return {"run_id": self.run_id, "status": "completed"}


class RecordingAoiPipeline:
    calls: ClassVar[list[str | None]] = []

    def run(self, run_id=None):
        self.calls.append(run_id)
        return _AoiResult(run_id or "generated")


def test_build_aoi_routes_root_and_run_id(monkeypatch, tmp_path: Path) -> None:
    RecordingAoiPipeline.calls = []
    roots: list[Path | None] = []

    def build(root=None):
        roots.append(root)
        return RecordingAoiPipeline()

    monkeypatch.setattr(CLI_MODULE, "build_study_area_pipeline", build)
    result = CliRunner().invoke(
        app,
        ["build-aoi", "--root", str(tmp_path), "--run-id", "aoi-1", "--json-summary"],
    )

    assert result.exit_code == 0, result.output
    assert roots == [tmp_path]
    assert RecordingAoiPipeline.calls == ["aoi-1"]
    assert json.loads(result.stdout) == {"run_id": "aoi-1", "status": "completed"}


class RecordingLandingService:
    calls: ClassVar[list[tuple[list[str], str | None]]] = []

    def run(self, source_ids, run_id=None):
        from flashflood_data.pipelines.static_landing.models import LandingRunSummary

        self.calls.append((list(source_ids), run_id))
        return LandingRunSummary(
            run_id=run_id or "generated",
            status="completed",
            completed_sources=tuple(source_ids),
        )


def test_landing_service_uses_project_root_environment(monkeypatch, tmp_path: Path) -> None:
    class StopAfterPathCheck(RuntimeError):
        pass

    def check_study_area_path(path: Path):
        assert path == tmp_path.resolve() / "config" / "study_area.yaml"
        raise StopAfterPathCheck

    monkeypatch.setenv("FLASHFLOOD_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setattr(LANDING_RUNTIME, "load_study_area", check_study_area_path)

    with pytest.raises(StopAfterPathCheck):
        LANDING_RUNTIME.build_static_landing_service()


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
                "sonla_admin_2025",
                "gadm_vnm_4_1",
                "hydrobasins_v1c",
                "basinatlas_v10",
                "hydrorivers_v10",
                "worldpop_vnm_2025",
                "geofabrik_vietnam_snapshot",
                "cop_dem_glo30_2024_1",
                "soilgrids_2_0",
                "esa_worldcover_2021_v200",
            ],
            None,
        )
    ]


def test_land_static_returns_nonzero_for_incomplete_run(monkeypatch, tmp_path: Path) -> None:
    from flashflood_data.pipelines.static_landing.models import LandingRunSummary

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
