"""Fixture-backed Copernicus DEM raw validation and continuous COG harmonization."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.sources.base import SourceContext
from flashflood_data.static.sources.cop_dem import CopDemAdapter


def _context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    aoi_dir = paths.harmonized / "aoi"
    aoi_dir.mkdir(parents=True, exist_ok=True)
    for name in ("environmental", "hydrological"):
        gpd.GeoDataFrame({"aoi": [name]}, geometry=[box(103, 20, 107, 22)], crs="EPSG:4326").to_parquet(
            aoi_dir / f"{name}_aoi.geoparquet", index=False
        )
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="cop-dem-integration-test",
    )


def _adapter() -> CopDemAdapter:
    return CopDemAdapter(
        SourceSpec(
            source_id="cop_dem_glo30_2024_1",
            adapter="cop_dem",
            version="2024_1",
            license_id="COP-DEM-30",
            settings={
                "token_url": "https://identity.example.test/token",
                "catalogue_url": "https://catalogue.example.test/odata/v1/Products",
                "download_template": "https://download.example.test/odata/v1/Products({product_id})/$value",
                "dataset": "COP-DEM_GLO-30-DGED/2024_1",
                "product_type": "SAR_DGE_30_A4AD",
            },
        )
    )


def _raw_asset(path: Path, asset_id: str) -> AssetRecord:
    return AssetRecord(
        asset_id=asset_id,
        source_id="cop_dem_glo30_2024_1",
        source_version="2024_1",
        kind=AssetKind.RAW,
        source_uri="https://download.example.test/odata/v1/Products(fixture)/$value",
        storage_path=str(path),
        media_type="image/tiff",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        license_id="COP-DEM-30",
        pipeline_run_id="cop-dem-integration-test",
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
        dtype="int16",
        crs="EPSG:4326",
        transform=from_origin(west, 22, 1, 1),
        nodata=-32768,
    ) as dataset:
        dataset.write(values, 1)


def test_dem_harmonize_preserves_raw_tiles_and_writes_float_cog(tmp_path: Path) -> None:
    context = _context(tmp_path)
    left = context.paths.raw / "cop_dem" / "2024_1" / "N20_E103" / "left_DEM.tif"
    right = context.paths.raw / "cop_dem" / "2024_1" / "N20_E104" / "right_DEM.tif"
    _write_tile(left, 103, np.array([[10, 20], [30, 40]], dtype="int16"))
    _write_tile(right, 105, np.array([[50, 60], [70, 80]], dtype="int16"))
    left_checksum, right_checksum = sha256_file(left), sha256_file(right)

    adapter = _adapter()
    assert adapter.validate_raw(left).passed
    outputs = adapter.harmonize(context, [_raw_asset(left, "dem-left"), _raw_asset(right, "dem-right")])

    assert sha256_file(left) == left_checksum
    assert sha256_file(right) == right_checksum
    assert len(outputs) == 1
    with rasterio.open(outputs[0].storage_path) as dataset:
        assert dataset.dtypes == ("float32",)
        assert dataset.profile["tiled"] is True
        assert set(dataset.read(1, masked=True).compressed()) == {10, 20, 30, 40, 50, 60, 70, 80}
