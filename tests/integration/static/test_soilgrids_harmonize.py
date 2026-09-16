"""Fixture-backed SoilGrids raster validation and harmonization."""

from __future__ import annotations

import json
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
from flashflood_data.static.sources.soilgrids import SoilGridsAdapter


def _context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    aoi_dir = paths.harmonized / "aoi"
    aoi_dir.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame({"aoi": ["environmental"]}, geometry=[box(104, 20, 105, 21)], crs="EPSG:4326").to_parquet(
        aoi_dir / "environmental_aoi.geoparquet", index=False
    )
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="soilgrids-integration-test",
    )


def _adapter() -> SoilGridsAdapter:
    return SoilGridsAdapter(
        SourceSpec(
            source_id="soilgrids_2_0",
            adapter="soilgrids",
            version="2.0",
            license_id="CC-BY-4.0",
            settings={
                "endpoint_template": "https://maps.isric.org/mapserv?map=/map/{property}.map",
                "properties": ["wv0033"],
                "depths": ["0-5cm"],
                "statistics": ["mean"],
                "format": "GEOTIFF_INT16",
                "output_crs": "EPSG:4326",
            },
        )
    )


def _raw_asset(path: Path) -> AssetRecord:
    return AssetRecord(
        asset_id="soilgrids-2-0-wv0033-0-5cm-mean",
        source_id="soilgrids_2_0",
        source_version="2.0",
        kind=AssetKind.RAW,
        source_uri="https://maps.isric.org/mapserv?REQUEST=GetCoverage",
        storage_path=str(path),
        media_type="image/tiff",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        license_id="CC-BY-4.0",
        pipeline_run_id="soilgrids-integration-test",
        status=AssetStatus.VALIDATED,
    )


def test_soilgrids_harmonize_keeps_int16_raw_values_and_scale_metadata(tmp_path: Path) -> None:
    context = _context(tmp_path)
    raw_path = context.paths.raw / "soilgrids" / "2.0" / "wv0033" / "0-5cm" / "mean.tif"
    raw_path.parent.mkdir(parents=True)
    data = np.array([[10, 20], [30, -32768]], dtype="int16")
    with rasterio.open(
        raw_path,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="int16",
        crs="EPSG:4326",
        transform=from_origin(104, 21, 0.5, 0.5),
        nodata=-32768,
    ) as dataset:
        dataset.write(data, 1)
    raw_checksum = sha256_file(raw_path)
    capabilities = context.paths.raw / "soilgrids" / "2.0" / "wv0033" / "capabilities.xml"
    capabilities.write_text("<Capabilities/>", encoding="utf-8")
    capability_asset = _raw_asset(capabilities).model_copy(
        update={
            "asset_id": "soilgrids-2-0-wv0033-capabilities",
            "media_type": "application/xml",
        }
    )

    adapter = _adapter()
    assert adapter.validate_raw(raw_path).passed
    outputs = adapter.harmonize(context, [capability_asset, _raw_asset(raw_path)])

    assert sha256_file(raw_path) == raw_checksum
    assert len(outputs) == 1
    output = outputs[0]
    with rasterio.open(output.storage_path) as dataset:
        assert dataset.dtypes == ("int16",)
        assert dataset.profile["tiled"] is True
        assert set(dataset.read(1, masked=True).compressed()) == {10, 20, 30}
    metadata = json.loads(output.metadata_json)
    assert metadata["property"] == "wv0033"
    assert metadata["unit"] == "cm3/cm3"
    assert metadata["raw_value_divisor"] == 10
    assert metadata["resampling"] == "bilinear"
