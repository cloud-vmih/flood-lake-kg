"""Integration tests for native-grid WorldPop Core-AOI harmonization."""

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
from flashflood_data.harmonize.exposure import harmonize_worldpop
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.base import SourceContext
from flashflood_data.sources.existing import ExistingAdapter


def test_worldpop_clip_preserves_included_native_pixel_counts(tmp_path: Path) -> None:
    raw = tmp_path / "worldpop.tif"
    with rasterio.open(
        raw,
        "w",
        driver="GTiff",
        width=4,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(103, 22, 1, 1),
        nodata=-9999.0,
    ) as dataset:
        dataset.write(np.array([[1, 2, 3, 4], [5, 6, 7, 8]], dtype="float32"), 1)

    output = harmonize_worldpop(raw, box(104, 20, 106, 22), tmp_path / "worldpop_2025.tif")

    with rasterio.open(output) as dataset:
        assert dataset.dtypes == ("float32",)
        assert dataset.transform == from_origin(104, 22, 1, 1)
        assert dataset.read(1, masked=True).sum() == 18
        assert dataset.profile["tiled"] is True


def test_worldpop_off_grid_aoi_uses_source_aligned_window_and_retains_exact_values(tmp_path: Path) -> None:
    raw = tmp_path / "worldpop.tif"
    source_transform = from_origin(103, 22, 1, 1)
    with rasterio.open(
        raw,
        "w",
        driver="GTiff",
        width=4,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=source_transform,
        nodata=-9999.0,
    ) as dataset:
        dataset.write(np.array([[1, 2, 3, 4], [5, 6, 7, 8]], dtype="float32"), 1)

    output = harmonize_worldpop(raw, box(104.25, 20.25, 105.75, 21.75), tmp_path / "worldpop_2025.tif")

    with rasterio.open(output) as dataset:
        assert dataset.transform == from_origin(104, 22, 1, 1)
        assert dataset.read(1, masked=True).filled(-9999.0).tolist() == [[2.0, 3.0], [6.0, 7.0]]


def test_existing_adapter_publishes_core_aoi_worldpop_record(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    aoi_path = paths.harmonized / "aoi" / "core_aoi.geoparquet"
    aoi_path.parent.mkdir(parents=True)
    gpd.GeoDataFrame({"aoi": ["core"]}, geometry=[box(104, 20, 106, 22)], crs="EPSG:4326").to_parquet(
        aoi_path, index=False
    )
    raw = paths.dataset / "Data" / "vnm_pop_2025_CN_100m_R2025A_v1.tif"
    raw.parent.mkdir(parents=True)
    with rasterio.open(
        raw,
        "w",
        driver="GTiff",
        width=4,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(103, 22, 1, 1),
        nodata=-9999.0,
    ) as dataset:
        dataset.write(np.array([[1, 2, 3, 4], [5, 6, 7, 8]], dtype="float32"), 1)
    context = SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="worldpop-integration-test",
    )
    raw_asset = AssetRecord(
        asset_id="worldpop-raw",
        source_id="worldpop_vnm_2025",
        source_version="R2025A-v1",
        kind=AssetKind.RAW,
        source_uri=raw.as_uri(),
        storage_path=str(raw),
        media_type="image/tiff",
        size_bytes=raw.stat().st_size,
        checksum=sha256_file(raw),
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        license_id="CC-BY-4.0",
        pipeline_run_id=context.run_id,
        status=AssetStatus.VALIDATED,
    )
    adapter = ExistingAdapter(
        SourceSpec(
            source_id="worldpop_vnm_2025",
            adapter="existing",
            version="R2025A-v1",
            license_id="CC-BY-4.0",
        )
    )

    outputs = adapter.harmonize(context, [raw_asset])

    assert [output.asset_id for output in outputs] == ["worldpop-vnm-2025-harmonized"]
    metadata = json.loads(outputs[0].metadata_json)
    assert metadata["pixel_inclusion_policy"] == "pixel-center inclusion in Core AOI"
    assert metadata["coverage_ratio_core_aoi"] == 1.0
    with rasterio.open(outputs[0].storage_path) as dataset:
        assert dataset.read(1, masked=True).sum() == 18


def test_existing_adapter_rejects_masked_core_aoi_worldpop_coverage(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    aoi_path = paths.harmonized / "aoi" / "core_aoi.geoparquet"
    aoi_path.parent.mkdir(parents=True)
    gpd.GeoDataFrame({"aoi": ["core"]}, geometry=[box(104, 20, 106, 22)], crs="EPSG:4326").to_parquet(
        aoi_path, index=False
    )
    raw = paths.dataset / "Data" / "vnm_pop_2025_CN_100m_R2025A_v1.tif"
    raw.parent.mkdir(parents=True)
    with rasterio.open(
        raw,
        "w",
        driver="GTiff",
        width=4,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(103, 22, 1, 1),
        nodata=-9999.0,
    ) as dataset:
        dataset.write(np.array([[1, -9999, 3, 4], [5, 6, 7, 8]], dtype="float32"), 1)
    context = SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="worldpop-coverage-test",
    )
    raw_asset = AssetRecord(
        asset_id="worldpop-raw",
        source_id="worldpop_vnm_2025",
        source_version="R2025A-v1",
        kind=AssetKind.RAW,
        source_uri=raw.as_uri(),
        storage_path=str(raw),
        media_type="image/tiff",
        size_bytes=raw.stat().st_size,
        checksum=sha256_file(raw),
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        license_id="CC-BY-4.0",
        pipeline_run_id=context.run_id,
        status=AssetStatus.VALIDATED,
    )
    adapter = ExistingAdapter(
        SourceSpec(
            source_id="worldpop_vnm_2025",
            adapter="existing",
            version="R2025A-v1",
            license_id="CC-BY-4.0",
        )
    )

    with pytest.raises(ValueError, match="Core AOI coverage"):
        adapter.harmonize(context, [raw_asset])
