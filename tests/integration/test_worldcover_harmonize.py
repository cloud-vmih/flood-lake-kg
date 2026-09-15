"""Fixture-backed categorical ESA WorldCover validation and harmonization."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.static.sources.base import SourceContext
from flashflood_data.static.sources.worldcover import (
    WorldCoverAdapter,
    validate_worldcover_classes,
)


def _context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    aoi_dir = paths.harmonized / "aoi"
    aoi_dir.mkdir(parents=True)
    for name in ("environmental", "hydrological"):
        gpd.GeoDataFrame({"aoi": [name]}, geometry=[box(102, 18, 106, 20)], crs="EPSG:4326").to_parquet(
            aoi_dir / f"{name}_aoi.geoparquet", index=False
        )
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="worldcover-integration-test",
    )


def _adapter() -> WorldCoverAdapter:
    return WorldCoverAdapter(
        SourceSpec(
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
    )


def _raw_asset(path: Path, asset_id: str) -> AssetRecord:
    return AssetRecord(
        asset_id=asset_id,
        source_id="esa_worldcover_2021_v200",
        source_version="2021-v200",
        kind=AssetKind.RAW,
        source_uri="https://worldcover.example.test/tile.tif",
        storage_path=str(path),
        media_type="image/tiff",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        license_id="CC-BY-4.0",
        pipeline_run_id="worldcover-integration-test",
        status=AssetStatus.VALIDATED,
    )


def _write_tile(path: Path, west: float, values: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(west, 20, 1, 1),
        nodata=0,
        tiled=True,
        blockxsize=16,
        blockysize=16,
    ) as dataset:
        dataset.write(values, 1)
        dataset.build_overviews([2], rasterio.enums.Resampling.nearest)


def test_worldcover_rejects_unknown_class(tmp_path: Path) -> None:
    path = tmp_path / "unknown-class.tif"
    _write_tile(path, 102, np.array([[10, 99], [80, 0]], dtype="uint8"))

    with pytest.raises(ValueError, match="unknown WorldCover class 99"):
        validate_worldcover_classes(path)


def test_worldcover_harmonize_preserves_raw_tiles_and_writes_nearest_cog(tmp_path: Path) -> None:
    context = _context(tmp_path)
    left = context.paths.raw / "worldcover" / "2021-v200" / "map" / "left.tif"
    right = context.paths.raw / "worldcover" / "2021-v200" / "map" / "right.tif"
    _write_tile(left, 102, np.array([[10, 80], [30, 40]], dtype="uint8"))
    _write_tile(right, 104, np.array([[50, 60], [90, 100]], dtype="uint8"))
    left_checksum, right_checksum = sha256_file(left), sha256_file(right)

    outputs = _adapter().harmonize(context, [_raw_asset(left, "worldcover-left"), _raw_asset(right, "worldcover-right")])

    assert sha256_file(left) == left_checksum
    assert sha256_file(right) == right_checksum
    assert len(outputs) == 1
    with rasterio.open(outputs[0].storage_path) as dataset:
        assert dataset.dtypes == ("uint8",)
        assert dataset.profile["tiled"] is True
        assert set(dataset.read(1, masked=True).compressed()) == {10, 30, 40, 50, 60, 80, 90, 100}
    metadata = json.loads(outputs[0].metadata_json)
    assert metadata["resampling"] == "nearest"
    assert metadata["water_pixels_hydrological_aoi"] == 1
