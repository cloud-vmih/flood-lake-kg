"""Contract tests for deterministic SoilGrids WCS acquisition."""

from __future__ import annotations

import gzip
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.sources.base import SourceConfigurationError, SourceContext
from flashflood_data.static.sources.soilgrids import SoilGridsAdapter, parse_coverages

PROPERTIES = ("clay", "sand", "silt", "bdod", "cfvo", "wv0010", "wv0033", "wv1500")
DEPTHS = ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")
STATISTICS = ("mean", "uncertainty")


@pytest.fixture
def fixture_dir() -> Path:
    return Path(__file__).parents[3] / "fixtures" / "soilgrids"


@pytest.fixture
def context(tmp_path: Path) -> SourceContext:
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
        run_id="soilgrids-contract-test",
    )


@pytest.fixture
def adapter() -> SoilGridsAdapter:
    return SoilGridsAdapter(
        SourceSpec(
            source_id="soilgrids_2_0",
            adapter="soilgrids",
            version="2.0",
            license_id="CC-BY-4.0",
            settings={
                "endpoint_template": "https://maps.isric.org/mapserv?map=/map/{property}.map",
                "properties": list(PROPERTIES),
                "depths": list(DEPTHS),
                "statistics": list(STATISTICS),
                "format": "GEOTIFF_INT16",
                "output_crs": "EPSG:4326",
                "target_resolution_m": 250,
                "capabilities_budget_size_bytes": 2_097_152,
                "coverage_budget_size_bytes": 67_108_864,
            },
        )
    )


def _available(path: Path, asset_id: str) -> AssetRecord:
    return AssetRecord(
        asset_id=asset_id,
        source_id="soilgrids_2_0",
        source_version="2.0",
        kind=AssetKind.RAW,
        source_uri="https://maps.isric.org/mapserv",
        storage_path=str(path),
        media_type="application/xml",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 23, tzinfo=UTC),
        license_id="CC-BY-4.0",
        pipeline_run_id="soilgrids-contract-test",
        status=AssetStatus.VALIDATED,
    )


def test_capabilities_include_verified_wv0033_mean_and_uncertainty(fixture_dir: Path) -> None:
    coverage_ids = parse_coverages((fixture_dir / "wv0033_capabilities.xml").read_bytes())

    assert "wv0033_0-5cm_mean" in coverage_ids
    assert "wv0033_100-200cm_uncertainty" in coverage_ids


def test_validate_raw_accepts_capabilities_xml(
    adapter: SoilGridsAdapter, fixture_dir: Path, tmp_path: Path
) -> None:
    capabilities = tmp_path / "wv0033" / "capabilities.xml"
    capabilities.parent.mkdir()
    capabilities.write_bytes((fixture_dir / "wv0033_capabilities.xml").read_bytes())

    result = adapter.validate_raw(capabilities)

    assert result.passed
    assert result.checks["capabilities_xml"]


def test_resolve_and_validation_accept_gzip_encoded_capabilities_without_renaming(
    adapter: SoilGridsAdapter, context: SourceContext, fixture_dir: Path, tmp_path: Path
) -> None:
    capabilities = tmp_path / "wv0033" / "capabilities.xml"
    capabilities.parent.mkdir()
    capabilities.write_bytes(
        gzip.compress((fixture_dir / "wv0033_capabilities.xml").read_bytes())
    )

    result = adapter.validate_raw(capabilities)
    remotes = adapter.resolve(
        context, [_available(capabilities, "soilgrids-2-0-wv0033-capabilities")]
    )

    assert result.passed
    assert len(remotes) == len(PROPERTIES) - 1


def test_resolve_declares_capabilities_before_coverage_requests(
    adapter: SoilGridsAdapter, context: SourceContext
) -> None:
    capabilities = adapter.resolve(context, [])

    assert len(capabilities) == len(PROPERTIES)
    assert {asset.target_relative_path for asset in capabilities} == {
        Path(f"raw/soilgrids/2.0/{property}/capabilities.xml") for property in PROPERTIES
    }
    assert all("REQUEST=GetCapabilities" in asset.uri for asset in capabilities)
    assert all(asset.budget_size_bytes == 2_097_152 for asset in capabilities)


def test_resolve_emits_eight_by_six_by_two_coverages(
    adapter: SoilGridsAdapter, context: SourceContext, fixture_dir: Path, tmp_path: Path
) -> None:
    template = (fixture_dir / "wv0033_capabilities.xml").read_text(encoding="utf-8")
    capability_assets = []
    for property_id in PROPERTIES:
        payload = tmp_path / f"{property_id}_capabilities.xml"
        payload.write_text(template.replace("wv0033", property_id), encoding="utf-8")
        capability_assets.append(_available(payload, f"soilgrids-2-0-{property_id}-capabilities"))

    assets = adapter.resolve(context, capability_assets)
    coverage_assets = [asset for asset in assets if "GetCoverage" in asset.uri]

    assert len(coverage_assets) == 8 * 6 * 2
    assert all(asset.target_relative_path.suffix == ".tif" for asset in coverage_assets)
    assert all(asset.target_relative_path.parts[:3] == ("raw", "soilgrids", "2.0") for asset in coverage_assets)
    request = parse_qs(urlsplit(coverage_assets[0].uri).query)
    assert request["SERVICE"] == ["WCS"]
    assert request["VERSION"] == ["2.0.1"]
    assert request["REQUEST"] == ["GetCoverage"]
    assert request["FORMAT"] == ["GEOTIFF_INT16"]
    assert request["OUTPUTCRS"] == ["EPSG:4326"]
    assert request["SUBSETTINGCRS"] == ["EPSG:4326"]
    assert request["SUBSET"] == ["Long(104,105)", "Lat(20,21)"]
    assert len(request["SCALESIZE"]) == 2
    assert request["SCALESIZE"][0].startswith("Long(")
    assert request["SCALESIZE"][1].startswith("Lat(")
    assert all(asset.budget_size_bytes == 67_108_864 for asset in coverage_assets)


def test_resolve_rejects_missing_soilgrids_download_bound(
    adapter: SoilGridsAdapter, context: SourceContext
) -> None:
    settings = dict(adapter.spec.settings)
    settings.pop("capabilities_budget_size_bytes")
    broken = SoilGridsAdapter(adapter.spec.model_copy(update={"settings": settings}))

    with pytest.raises(SourceConfigurationError, match="capabilities_budget_size_bytes"):
        broken.resolve(context, [])


def test_resolve_rejects_property_missing_an_uncertainty_coverage(
    adapter: SoilGridsAdapter, context: SourceContext, fixture_dir: Path, tmp_path: Path
) -> None:
    payload = tmp_path / "wv0033_capabilities.xml"
    missing = "<wcs:CoverageSummary><wcs:CoverageId>wv0033_100-200cm_uncertainty</wcs:CoverageId></wcs:CoverageSummary>"
    payload.write_text(
        (fixture_dir / "wv0033_capabilities.xml").read_text(encoding="utf-8").replace(missing, ""),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing configured coverages"):
        adapter.resolve(context, [_available(payload, "soilgrids-2-0-wv0033-capabilities")])


def test_resolve_reuses_covering_raw_and_versions_expanded_bbox(
    adapter: SoilGridsAdapter, context: SourceContext, fixture_dir: Path
) -> None:
    settings = dict(adapter.spec.settings)
    settings.update(properties=["wv0033"], depths=["0-5cm"], statistics=["mean"])
    adapter = SoilGridsAdapter(adapter.spec.model_copy(update={"settings": settings}))
    capabilities = context.paths.raw / "soilgrids" / "2.0" / "wv0033" / "capabilities.xml"
    capabilities.parent.mkdir(parents=True)
    capabilities.write_bytes((fixture_dir / "wv0033_capabilities.xml").read_bytes())
    capability = _available(capabilities, "soilgrids-2-0-wv0033-capabilities")
    legacy_path = capabilities.parent / "0-5cm" / "mean.tif"
    legacy_path.parent.mkdir()

    def write_raster(path: Path, west: float, east: float) -> None:
        with rasterio.open(
            path, "w", driver="GTiff", width=10, height=10, count=1,
            dtype="int16", crs="EPSG:4326", nodata=-32768,
            transform=from_origin(west, 21, (east - west) / 10, 0.1),
        ) as dataset:
            dataset.write(np.ones((10, 10), dtype="int16"), 1)

    write_raster(legacy_path, 104, 105)
    legacy = _available(legacy_path, "soilgrids-2-0-wv0033-0-5cm-mean").model_copy(
        update={"media_type": "image/tiff"}
    )
    available = [capability, legacy]
    assert adapter.resolve(context, available)[0].asset_id == legacy.asset_id

    aoi_path = context.paths.harmonized / "aoi" / "environmental_aoi.geoparquet"
    gpd.GeoDataFrame(geometry=[box(103.5, 20, 105, 21)], crs="EPSG:4326").to_parquet(
        aoi_path, index=False
    )
    expanded = adapter.resolve(context, available)[0]
    assert expanded.asset_id != legacy.asset_id
    assert expanded.target_relative_path != Path("raw/soilgrids/2.0/wv0033/0-5cm/mean.tif")
    assert "Long(103.5,105)" in expanded.uri

    expanded_path = context.paths.dataset / expanded.target_relative_path
    write_raster(expanded_path, 104, 105)
    insufficient = _available(expanded_path, expanded.asset_id).model_copy(
        update={"media_type": "image/tiff", "source_uri": expanded.uri}
    )
    with pytest.raises(ValueError, match="does not cover"):
        adapter.resolve(context, [*available, insufficient])

    write_raster(expanded_path, 103.5, 105)
    expanded_record = _available(expanded_path, expanded.asset_id).model_copy(
        update={"media_type": "image/tiff", "source_uri": expanded.uri}
    )
    assert adapter.resolve(context, [*available, expanded_record])[0].asset_id == expanded.asset_id

    gpd.GeoDataFrame(geometry=[box(104.1, 20.1, 104.9, 20.9)], crs="EPSG:4326").to_parquet(
        aoi_path, index=False
    )
    assert adapter.resolve(context, [*available, expanded_record])[0].asset_id == legacy.asset_id
