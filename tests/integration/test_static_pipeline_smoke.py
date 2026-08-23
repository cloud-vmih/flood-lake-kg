"""The advertised CLI composes every reviewed static stage by default."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.cli import app
from flashflood_data.paths import ProjectPaths
from tests.fixtures.static_pipeline.build_fixture_lake import (
    build_fixture_lake,
    isolate_external_boundaries,
)

_EXPECTED_QA_ASSETS = {
    "task17-qa-map-data-bridges-geojson",
    "task17-qa-map-data-communes-geojson",
    "task17-qa-map-data-facilities-geojson",
    "task17-qa-map-data-rivers-geojson",
    "task17-qa-map-data-roads-geojson",
    "task17-qa-map-data-settlements-geojson",
    "task17-qa-map-data-subbasins_l10-geojson",
    "task17-qa-map-index-html",
    "task17-qa-map-layer-manifest-json",
    "task17-qa-map-previews-dem-png",
    "task17-qa-map-previews-worldcover-png",
    "task17-qa-map-previews-worldpop-png",
    "task17-qa-report-html",
    "task17-qa-report-json",
    "task17-qa-report-parquet",
}


def test_static_pipeline_smoke_creates_profile_and_passing_qa(
    tmp_path: Path, monkeypatch
) -> None:
    """Omitting default registration would leave the CLI green without producing final assets."""
    isolate_external_boundaries(monkeypatch)
    paths = build_fixture_lake(tmp_path)

    result = CliRunner().invoke(
        app, ["run-static", "--profile", "smoke", "--root", str(tmp_path), "--json-summary"]
    )

    assert result.exit_code == 0, result.output
    assert (paths.derived / "subbasin_static_feature.geoparquet").is_file()
    assert (paths.qa / "map" / "index.html").is_file()
    report = json.loads((paths.qa / "report.json").read_text(encoding="utf-8"))
    assert not [
        check
        for check in report["checks"]
        if check["severity"] == "fatal" and not check["passed"]
    ]
    qa_assets = {
        asset.asset_id: asset
        for asset in AssetCatalog(ProjectPaths.discover(tmp_path))._read_assets()
        if asset.asset_id.startswith("task17-qa-")
    }
    assert set(qa_assets) == _EXPECTED_QA_ASSETS
    assert all(
        asset.checksum == sha256_file(Path(asset.storage_path)) for asset in qa_assets.values()
    )
