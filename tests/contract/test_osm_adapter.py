"""Contract tests for timestamped, immutable Geofabrik OSM snapshot resolution."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.sources.base import SourceContext
from flashflood_data.sources.osm import GeofabrikOsmAdapter, parse_geofabrik_metadata


def _spec() -> SourceSpec:
    return SourceSpec(
        source_id="geofabrik_vietnam_snapshot",
        adapter="geofabrik_osm",
        version="snapshot",
        license_id="ODbL-1.0",
        settings={
            "pbf_url": "https://geofabrik.example.test/asia/vietnam-latest.osm.pbf",
            "md5_url": "https://geofabrik.example.test/asia/vietnam-latest.osm.pbf.md5",
        },
    )


def _context(tmp_path: Path) -> SourceContext:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    aoi_path = paths.harmonized / "aoi" / "exposure_aoi.geoparquet"
    aoi_path.parent.mkdir(parents=True)
    gpd.GeoDataFrame({"name": ["exposure"]}, geometry=gpd.GeoSeries.from_wkt(["POLYGON ((103 20, 104 20, 104 21, 103 21, 103 20))"]), crs="EPSG:4326").to_parquet(aoi_path, index=False)
    return SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="osm-contract-test",
    )


def _sidecar_asset(path: Path, spec: SourceSpec) -> AssetRecord:
    return AssetRecord(
        asset_id="geofabrik-osm-20260820-md5",
        source_id=spec.source_id,
        source_version=spec.version,
        kind=AssetKind.RAW,
        source_uri=str(spec.settings["md5_url"]),
        storage_path=str(path),
        media_type="text/plain",
        size_bytes=path.stat().st_size,
        checksum=sha256_file(path),
        retrieved_at=datetime(2026, 8, 20, tzinfo=UTC),
        source_valid_time="2026-08-20T03:04:05+00:00",
        license_id=spec.license_id,
        pipeline_run_id="osm-contract-test",
        status=AssetStatus.VALIDATED,
    )


def test_snapshot_target_uses_last_modified_date() -> None:
    metadata = parse_geofabrik_metadata(
        {"Last-Modified": "Thu, 20 Aug 2026 03:04:05 GMT", "Content-Length": "4096"},
        "b1946ac92492d2347c6235b4d2611184  vietnam-latest.osm.pbf\n",
    )

    assert metadata.target_name == "vietnam-20260820.osm.pbf"
    assert metadata.md5 == "b1946ac92492d2347c6235b4d2611184"
    assert metadata.content_length == 4096
    assert metadata.source_valid_time == datetime(2026, 8, 20, 3, 4, 5, tzinfo=UTC)


def test_resolve_downloads_sidecar_before_exposing_timestamped_pbf(tmp_path: Path) -> None:
    context, spec = _context(tmp_path), _spec()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith(".pbf"):
            return httpx.Response(
                200,
                headers={"Last-Modified": "Thu, 20 Aug 2026 03:04:05 GMT", "Content-Length": "4096"},
            )
        return httpx.Response(200, headers={"Content-Length": "57"})

    adapter = GeofabrikOsmAdapter(spec, client=httpx.Client(transport=httpx.MockTransport(handler)))
    sidecar = adapter.resolve(context, [])

    assert [remote.asset_id for remote in sidecar] == ["geofabrik-osm-20260820-md5"]
    assert sidecar[0].target_relative_path == Path("raw/osm/geofabrik/20260820/vietnam-20260820.osm.pbf.md5")
    assert [request.method for request in requests] == ["HEAD", "HEAD"]

    path = context.paths.dataset / sidecar[0].target_relative_path
    path.parent.mkdir(parents=True)
    path.write_text("b1946ac92492d2347c6235b4d2611184  vietnam-latest.osm.pbf\n", encoding="ascii")
    pbf = adapter.resolve(context, [_sidecar_asset(path, spec)])

    assert [remote.asset_id for remote in pbf] == ["geofabrik-osm-20260820-pbf"]
    assert pbf[0].target_relative_path == Path("raw/osm/geofabrik/20260820/vietnam-20260820.osm.pbf")
    assert pbf[0].expected_size == 4096
    assert pbf[0].source_valid_time == "2026-08-20T03:04:05+00:00"
