"""Contract tests for deterministic SoilGrids WCS acquisition."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import geopandas as gpd
import pytest
from shapely.geometry import box

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.base import SourceContext
from flashflood_data.sources.soilgrids import SoilGridsAdapter, parse_coverages

PROPERTIES = ("clay", "sand", "silt", "bdod", "cfvo", "wv0010", "wv0033", "wv1500")
DEPTHS = ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")
STATISTICS = ("mean", "uncertainty")


@pytest.fixture
def fixture_dir() -> Path:
    return Path(__file__).parents[1] / "fixtures" / "soilgrids"


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


def test_resolve_declares_capabilities_before_coverage_requests(
    adapter: SoilGridsAdapter, context: SourceContext
) -> None:
    capabilities = adapter.resolve(context, [])

    assert len(capabilities) == len(PROPERTIES)
    assert {asset.target_relative_path for asset in capabilities} == {
        Path(f"raw/soilgrids/2.0/{property}/capabilities.xml") for property in PROPERTIES
    }
    assert all("REQUEST=GetCapabilities" in asset.uri for asset in capabilities)


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
    assert request["SUBSET"] == ["Long(104,105)", "Lat(20,21)"]


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
