"""Contract tests for immutable ESA WorldCover 2021 v200 acquisition."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx
import pytest
from shapely.geometry import box
from shapely.ops import unary_union

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.static.sources.base import SourceContext
from flashflood_data.static.sources.worldcover import (
    WorldCoverAdapter,
    select_worldcover_tiles,
)


@pytest.fixture
def grid_fixture() -> Path:
    return Path(__file__).parents[1] / "fixtures" / "worldcover" / "grid.geojson"


@pytest.fixture
def environmental_aoi():
    return unary_union([box(102.1, 18.1, 102.2, 18.2), box(102.1, 21.1, 102.2, 21.2)])


@pytest.fixture
def spec() -> SourceSpec:
    return SourceSpec(
        source_id="esa_worldcover_2021_v200",
        adapter="worldcover",
        version="2021-v200",
        license_id="CC-BY-4.0",
        settings={
            "grid_url": "https://worldcover.example.test/grid.geojson",
            "tile_template": "https://worldcover.example.test/{tile}.tif",
            "valid_classes": [10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100],
            "nodata": 0,
        },
    )


@pytest.fixture
def context(tmp_path: Path, environmental_aoi) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    aoi_dir = paths.harmonized / "aoi"
    aoi_dir.mkdir(parents=True)
    gpd.GeoDataFrame({"aoi": ["environmental"]}, geometry=[environmental_aoi], crs="EPSG:4326").to_parquet(
        aoi_dir / "environmental_aoi.geoparquet", index=False
    )
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="worldcover-contract-test",
    )


def _grid_asset(path: Path, spec: SourceSpec) -> AssetRecord:
    return AssetRecord(
        asset_id="worldcover-2021-v200-grid",
        source_id=spec.source_id,
        source_version=spec.version,
        kind=AssetKind.RAW,
        source_uri=str(spec.settings["grid_url"]),
        storage_path=str(path),
        media_type="application/geo+json",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        license_id=spec.license_id,
        pipeline_run_id="worldcover-contract-test",
        status=AssetStatus.VALIDATED,
    )


def test_only_intersecting_worldcover_tiles_are_resolved(
    grid_fixture: Path, environmental_aoi
) -> None:
    tiles = select_worldcover_tiles(grid_fixture, environmental_aoi)

    assert [tile.tile_id for tile in tiles] == ["N18E102", "N21E102"]


def test_resolve_preserves_grid_before_selecting_tiles(context: SourceContext, spec: SourceSpec) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, headers={"Content-Length": "4096"})

    remotes = WorldCoverAdapter(spec, client=httpx.Client(transport=httpx.MockTransport(handler))).resolve(context, [])

    assert [remote.asset_id for remote in remotes] == ["worldcover-2021-v200-grid"]
    assert remotes[0].target_relative_path == Path("raw/worldcover/2021-v200/grid.geojson")
    assert remotes[0].expected_size == 4096
    assert [request.method for request in seen] == ["HEAD"]


def test_resolve_heads_only_sorted_intersecting_tiles(
    context: SourceContext, grid_fixture: Path, spec: SourceSpec
) -> None:
    grid_path = context.paths.raw / "worldcover" / "2021-v200" / "grid.geojson"
    grid_path.parent.mkdir(parents=True)
    grid_path.write_bytes(grid_fixture.read_bytes())
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, headers={"Content-Length": "2048"})

    adapter = WorldCoverAdapter(spec, client=httpx.Client(transport=httpx.MockTransport(handler)))
    remotes = adapter.resolve(context, [_grid_asset(grid_path, spec)])

    assert [remote.asset_id for remote in remotes] == [
        "worldcover-2021-v200-N18E102",
        "worldcover-2021-v200-N21E102",
    ]
    assert [request.method for request in seen] == ["HEAD", "HEAD"]
    assert [str(request.url) for request in seen] == [
        "https://worldcover.example.test/N18E102.tif",
        "https://worldcover.example.test/N21E102.tif",
    ]
    assert all(remote.expected_size == 2048 for remote in remotes)


def test_select_worldcover_tiles_rejects_missing_tile_identifier(tmp_path: Path, environmental_aoi) -> None:
    grid = tmp_path / "grid.geojson"
    grid.write_text(json.dumps({"type": "FeatureCollection", "features": []}), encoding="utf-8")

    with pytest.raises(ValueError, match="ll_tile"):
        select_worldcover_tiles(grid, environmental_aoi)


def test_validate_raw_accepts_retained_worldcover_grid(
    grid_fixture: Path, spec: SourceSpec
) -> None:
    result = WorldCoverAdapter(spec).validate_raw(grid_fixture)

    assert result.passed
    assert result.checks["grid_schema"]
