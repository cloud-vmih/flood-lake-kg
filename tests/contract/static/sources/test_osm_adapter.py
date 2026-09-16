"""Contract tests for timestamped, immutable Geofabrik OSM snapshot resolution."""

from __future__ import annotations

from configparser import ConfigParser
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import httpx
from shapely.geometry import box

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.sources.base import SourceContext
from flashflood_data.static.sources.osm import (
    GeofabrikOsmAdapter,
    _read_layers,
    parse_geofabrik_metadata,
)


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


def test_validate_raw_accepts_md5_sidecar(tmp_path: Path) -> None:
    sidecar = tmp_path / "vietnam-20260820.osm.pbf.md5"
    sidecar.write_text(
        "b1946ac92492d2347c6235b4d2611184  vietnam-latest.osm.pbf\n",
        encoding="ascii",
    )

    result = GeofabrikOsmAdapter(_spec()).validate_raw(sidecar)

    assert result.passed
    assert result.checks["md5_sidecar"]


def test_osm_driver_config_preserves_source_ids_for_every_read_layer() -> None:
    config = ConfigParser()
    config.read_string(
        "[global]\n"
        + (Path(__file__).parents[4] / "config" / "osmconf.ini").read_text(encoding="utf-8")
    )

    for layer in ("points", "lines", "multilinestrings", "multipolygons"):
        attributes = {
            value.strip() for value in config[layer]["attributes"].split(",")
        }
        assert config[layer].getboolean("osm_id")
        assert "osm_id" not in attributes
        assert "other_tags" not in attributes
        assert config[layer].getboolean("other_tags")


def test_osm_driver_classifies_closed_amenity_way_as_polygon(tmp_path: Path) -> None:
    osm = tmp_path / "closed-school.osm"
    osm.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="flashflood-test">
  <node id="1" lat="20.0" lon="103.0" />
  <node id="2" lat="20.0" lon="103.1" />
  <node id="3" lat="20.1" lon="103.1" />
  <node id="4" lat="20.1" lon="103.0" />
  <way id="10">
    <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
    <tag k="amenity" v="school"/>
    <tag k="name" v="Fixture School"/>
  </way>
</osm>
""",
        encoding="utf-8",
    )

    layers = _read_layers(
        osm,
        box(102.9, 19.9, 103.2, 20.2),
        Path(__file__).parents[4] / "config" / "osmconf.ini",
    )

    polygons = layers["multipolygons"]
    assert polygons["amenity"].tolist() == ["school"]
    assert polygons["osm_way_id"].tolist() == ["10"]
