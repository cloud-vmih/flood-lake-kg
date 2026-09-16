"""Build a tiny, fully local static lake for CLI-level integration tests."""

from __future__ import annotations

import socket
from collections import namedtuple
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import LineString, Point, box

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.paths import ProjectPaths
from flashflood_data.static.features.config import load_feature_config

_NOW = datetime(2026, 8, 21, tzinfo=UTC)
_TRANSFORM = from_origin(104.0, 20.01, 0.001, 0.001)
_RASTER_SHAPE = (10, 10)


def isolate_external_boundaries(monkeypatch) -> None:
    """Reject live sockets and make the host disk-reserve boundary deterministic."""

    def blocked_connect(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("pytest attempted a live network connection")

    disk_usage = namedtuple("disk_usage", "total used free")
    monkeypatch.setattr(socket.socket, "connect", blocked_connect)
    monkeypatch.setattr(
        "flashflood_data.qa.checks.shutil.disk_usage",
        lambda _: disk_usage(40 * 2**30, 10 * 2**30, 30 * 2**30),
    )


def _write_geoparquet(frame: gpd.GeoDataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _write_shapefile(frame: gpd.GeoDataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_file(path)
    return path


def _write_raster(
    path: Path,
    values: np.ndarray,
    *,
    dtype: str,
    nodata: float,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=_RASTER_SHAPE[1],
        height=_RASTER_SHAPE[0],
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=_TRANSFORM,
        nodata=nodata,
    ) as destination:
        destination.write(values.astype(dtype), 1)
    return path


def _register_raw(
    catalog: AssetCatalog,
    path: Path,
    *,
    asset_id: str,
    source_id: str,
    source_version: str,
    media_type: str,
    license_id: str = "fixture-open-license",
) -> None:
    catalog.upsert(
        AssetRecord(
            asset_id=asset_id,
            source_id=source_id,
            source_version=source_version,
            kind=AssetKind.RAW,
            source_uri=f"https://fixture.invalid/{asset_id}",
            storage_path=str(path),
            media_type=media_type,
            size_bytes=path.stat().st_size,
            checksum=sha256_file(path),
            retrieved_at=_NOW,
            license_id=license_id,
            pipeline_run_id="fixture-bootstrap",
            status=AssetStatus.VALIDATED,
        )
    )


def _write_configuration(root: Path) -> None:
    config = root / "config"
    sources = config / "sources"
    sources.mkdir(parents=True, exist_ok=True)
    (config / "study_area.yaml").write_text(
        """province_origin_code: "14"
hydrobasins_level: 10
upstream_hops: 1
raster_buffer_km: 10
exposure_buffer_km: 10
processing_crs: "EPSG:32648"
storage_crs: "EPSG:4326"
new_raw_soft_cap_gib: 8
minimum_free_gib: 10
admin_expected_count: 3
admin_expected_communes: 2
admin_expected_wards: 1
historical_event_expected_count: 2
admin_gap_overlap_max_pct: 0.1
legal_area_diff_max_pct: 2.0
commune_basin_coverage_min_pct: 99.5
commune_basin_coverage_max_pct: 100.5
environmental_raster_coverage_min_pct: 99.0
admin_area_exceptions: []
""",
        encoding="utf-8",
    )
    (sources / "existing.yaml").write_text(
        """sources:
  - source_id: worldpop_vnm_2025
    adapter: existing
    version: R2025A-v1
    license_id: CC-BY-4.0
    settings: {inventory_source_id: worldpop_vnm_2025}
""",
        encoding="utf-8",
    )


def _admin_layers(paths: ProjectPaths) -> None:
    geometries = [
        box(104.000, 20.000, 104.002, 20.006),
        box(104.002, 20.000, 104.004, 20.006),
        box(104.004, 20.000, 104.006, 20.006),
    ]
    admin = gpd.GeoDataFrame(
        {
            "current_commune_code": ["14001", "14002", "14003"],
            "current_commune_name": ["Alpha", "Beta", "Gamma"],
            "unit_type": ["commune", "commune", "ward"],
            "geometry_repaired": [False, False, False],
        },
        geometry=geometries,
        crs="EPSG:4326",
    )
    admin["legal_area_km2"] = admin.to_crs("EPSG:32648").geometry.area / 1_000_000
    _write_geoparquet(admin, paths.harmonized / "admin" / "admin_commune_2025.geoparquet")
    reference = gpd.GeoDataFrame(
        {"reference_id": ["sonla-fixture"]},
        geometry=[admin.geometry.union_all()],
        crs=admin.crs,
    )
    _write_geoparquet(
        reference, paths.harmonized / "admin" / "sonla_reference_boundary.geoparquet"
    )
    historical = gpd.GeoDataFrame(
        {
            "historical_commune_id": ["old-1", "old-2", "old-3", "old-4"],
            "historical_commune_name": ["Old Alpha", "Old Beta", "Old Gamma", "Old Delta"],
            "match_status": ["matched", "matched", "ambiguous", "unresolved"],
        },
        geometry=[
            box(104.000, 20.000, 104.0015, 20.006),
            box(104.0015, 20.000, 104.003, 20.006),
            box(104.003, 20.000, 104.0045, 20.006),
            box(104.0045, 20.000, 104.006, 20.006),
        ],
        crs="EPSG:4326",
    )
    _write_geoparquet(
        historical, paths.harmonized / "admin" / "admin_commune_historical.geoparquet"
    )
    _write_geoparquet(
        gpd.GeoDataFrame(
            {"aoi_id": ["core-aoi-2025"]}, geometry=[admin.geometry.union_all()], crs=admin.crs
        ),
        paths.harmonized / "aoi" / "core_aoi.geoparquet",
    )
    _write_geoparquet(
        gpd.GeoDataFrame(
            {"country_id": ["VNM"]},
            geometry=[box(103.99, 19.99, 104.02, 20.02)],
            crs="EPSG:4326",
        ),
        paths.harmonized / "admin" / "vietnam_boundary.geoparquet",
    )
    _write_parquet(
        pd.DataFrame(
            {
                "historical_commune_id": historical["historical_commune_id"],
                "current_commune_code": ["14001", "14002", "14002", pd.NA],
                "match_status": historical["match_status"],
                "match_confidence": [1.0, 0.9, 0.5, 0.0],
            }
        ),
        paths.derived / "mappings" / "admin_commune_crosswalk.parquet",
    )


def _hydro_layers(paths: ProjectPaths) -> None:
    basin_geometries = [
        box(104.0000, 20.000, 104.0030, 20.006),
        box(104.0030, 20.000, 104.0060, 20.006),
        box(104.0061, 20.000, 104.0090, 20.006),
    ]
    l10 = gpd.GeoDataFrame(
        {
            "HYBAS_ID": [101, 102, 103],
            "PFAF_ID": [1111, 1112, 1113],
            "NEXT_DOWN": [0, 0, 102],
            "SUB_AREA": [0.2, 0.2, 0.2],
            "UP_AREA": [0.2, 0.4, 0.2],
        },
        geometry=basin_geometries,
        crs="EPSG:4326",
    )
    l9 = gpd.GeoDataFrame(
        {"HYBAS_ID": [91], "PFAF_ID": [111]},
        geometry=[box(103.999, 19.999, 104.010, 20.007)],
        crs="EPSG:4326",
    )
    l8 = gpd.GeoDataFrame(
        {"HYBAS_ID": [81], "PFAF_ID": [11]},
        geometry=[box(103.998, 19.998, 104.011, 20.008)],
        crs="EPSG:4326",
    )
    legacy = paths.dataset / "hybas_as_lev01-12_v1c"
    _write_shapefile(l10, legacy / "hybas_as_lev10_v1c.shp")
    _write_shapefile(l9, legacy / "hybas_as_lev09_v1c.shp")
    _write_shapefile(l8, legacy / "hybas_as_lev08_v1c.shp")
    _write_geoparquet(l10, paths.harmonized / "hydro" / "subbasin_l10.geoparquet")
    _write_parquet(
        pd.DataFrame(
            {
                "HYBAS_ID": [101, 102, 103],
                "parent_l9_hybas_id": [91, 91, 91],
                "parent_l8_hybas_id": [81, 81, 81],
                "scope_exit": [False, False, False],
            }
        ),
        paths.harmonized / "hydro" / "subbasin_hierarchy.parquet",
    )
    semantics = load_feature_config()
    atlas_values: dict[str, list[float | int]] = {
        "HYBAS_ID": [101, 102, 103],
    }
    atlas_values.update(
        {field: [float(index + 1)] * 3 for index, field in enumerate(semantics.basinatlas_fields)}
    )
    atlas = gpd.GeoDataFrame(atlas_values, geometry=basin_geometries, crs="EPSG:4326")
    atlas_path = (
        paths.dataset
        / "BasinATLAS_Data_v10_shp"
        / "BasinATLAS_v10_shp"
        / "BasinATLAS_v10_lev10.shp"
    )
    _write_shapefile(atlas, atlas_path)
    _write_geoparquet(atlas, paths.harmonized / "hydro" / "basinatlas_l10.geoparquet")
    rivers = gpd.GeoDataFrame(
        {"HYRIV_ID": [7001, 7002]},
        geometry=[
            LineString([(104.0002, 20.003), (104.0058, 20.003)]),
            LineString([(104.0062, 20.003), (104.0088, 20.003)]),
        ],
        crs="EPSG:4326",
    )
    river_path = (
        paths.dataset
        / "HydroRIVERS_v10_as_shp"
        / "HydroRIVERS_v10_as_shp"
        / "HydroRIVERS_v10_as.shp"
    )
    _write_shapefile(rivers, river_path)
    _write_geoparquet(rivers, paths.harmonized / "hydro" / "river_reach.geoparquet")


def _exposure_layers(paths: ProjectPaths) -> None:
    layers = {
        "road_segment": gpd.GeoDataFrame(
            {"segment_id": ["way/1:000"], "osm_id": ["way/1"]},
            geometry=[LineString([(104.0002, 20.001), (104.0058, 20.001)])],
            crs="EPSG:4326",
        ),
        "bridge": gpd.GeoDataFrame(
            {"osm_id": ["way/2"]},
            geometry=[LineString([(104.0025, 20.004), (104.0035, 20.004)])],
            crs="EPSG:4326",
        ),
        "facility": gpd.GeoDataFrame(
            {"osm_id": ["node/1"], "amenity": ["clinic"]},
            geometry=[Point(104.001, 20.002)],
            crs="EPSG:4326",
        ),
        "settlement": gpd.GeoDataFrame(
            {"osm_id": ["node/2"], "place": ["village"]},
            geometry=[Point(104.004, 20.002)],
            crs="EPSG:4326",
        ),
        "water_context": gpd.GeoDataFrame(
            {"osm_id": ["relation/3"]},
            geometry=[box(104.007, 20.001, 104.008, 20.002)],
            crs="EPSG:4326",
        ),
    }
    for name, layer in layers.items():
        _write_geoparquet(layer, paths.harmonized / "exposure" / f"{name}.geoparquet")


def _raster_layers(paths: ProjectPaths, catalog: AssetCatalog) -> None:
    dem = np.arange(100, 200, dtype="float32").reshape(_RASTER_SHAPE)
    raw_dem = _write_raster(
        paths.raw / "dem" / "fixture-dem.tif", dem, dtype="float32", nodata=-9999.0
    )
    _register_raw(
        catalog,
        raw_dem,
        asset_id="raw-fixture-dem",
        source_id="cop_dem_glo30_2024_1",
        source_version="2024_1",
        media_type="image/tiff",
    )
    _write_raster(
        paths.harmonized / "rasters" / "dem_glo30.tif",
        dem,
        dtype="float32",
        nodata=-9999.0,
    )

    semantics = load_feature_config()
    for property_index, property_id in enumerate(semantics.soil_properties):
        for depth_index, depth in enumerate(semantics.soil_depths):
            for statistic_index, statistic in enumerate(semantics.soil_statistics):
                values = np.full(
                    _RASTER_SHAPE,
                    100 + property_index * 10 + depth_index * 2 + statistic_index,
                    dtype="int16",
                )
                raw = _write_raster(
                    paths.raw
                    / "soilgrids"
                    / "2.0"
                    / property_id
                    / depth
                    / f"{statistic}.tif",
                    values,
                    dtype="int16",
                    nodata=-32768,
                )
                _register_raw(
                    catalog,
                    raw,
                    asset_id=f"raw-fixture-soil-{property_id}-{depth}-{statistic}",
                    source_id="soilgrids_2_0",
                    source_version="2.0",
                    media_type="image/tiff",
                )
                _write_raster(
                    paths.harmonized
                    / "soilgrids"
                    / property_id
                    / depth
                    / f"{statistic}.tif",
                    values,
                    dtype="int16",
                    nodata=-32768,
                )

    worldcover_values = np.full(_RASTER_SHAPE, 10, dtype="uint8")
    raw_worldcover = _write_raster(
        paths.raw / "worldcover" / "fixture-worldcover.tif",
        worldcover_values,
        dtype="uint8",
        nodata=0,
    )
    _register_raw(
        catalog,
        raw_worldcover,
        asset_id="raw-fixture-worldcover",
        source_id="esa_worldcover_2021_v200",
        source_version="2021-v200",
        media_type="image/tiff",
    )
    _write_raster(
        paths.harmonized / "rasters" / "worldcover_2021.tif",
        worldcover_values,
        dtype="uint8",
        nodata=0,
    )

    worldpop_values = np.ones(_RASTER_SHAPE, dtype="float32")
    raw_worldpop = _write_raster(
        paths.raw / "worldpop" / "fixture-worldpop.tif",
        worldpop_values,
        dtype="float32",
        nodata=-9999.0,
    )
    _register_raw(
        catalog,
        raw_worldpop,
        asset_id="raw-fixture-worldpop",
        source_id="worldpop_vnm_2025",
        source_version="R2025A-v1",
        media_type="image/tiff",
        license_id="CC-BY-4.0",
    )


def _other_raw_assets(paths: ProjectPaths, catalog: AssetCatalog) -> None:
    payloads = (
        (
            "raw-fixture-admin",
            "sonla_admin_2025",
            "2025-07-01",
            paths.raw / "admin" / "fixture-admin.json",
            b'{"fixture":"current-admin"}',
            "application/json",
        ),
        (
            "raw-fixture-osm",
            "geofabrik_vietnam_snapshot",
            "2026-08-21",
            paths.raw / "osm" / "vietnam-2026-08-21.osm.pbf",
            b"fixture-osm-pbf",
            "application/vnd.openstreetmap.data+pbf",
        ),
    )
    for asset_id, source_id, version, path, content, media_type in payloads:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        _register_raw(
            catalog,
            path,
            asset_id=asset_id,
            source_id=source_id,
            source_version=version,
            media_type=media_type,
        )


def _event_layer(paths: ProjectPaths, *, broken: bool) -> None:
    count = 1 if broken else 2
    events = pd.DataFrame(
        {
            "event_id": [f"event-{index + 1}" for index in range(count)],
            "event_date": [f"202{index}-08-01" for index in range(count)],
            "original_text": [f"Fixture flood report {index + 1}" for index in range(count)],
            "match_status": ["matched"] * count,
            "match_confidence": [1.0] * count,
            "candidate_evidence_json": ['[{"code":"14001"}]'] * count,
        }
    )
    _write_parquet(
        events, paths.harmonized / "events" / "historical_flood_event_2020_2026.parquet"
    )


def build_fixture_lake(root: Path, *, broken: bool = False) -> ProjectPaths:
    """Create a complete tiny source/harmonized lake without any network access."""
    _write_configuration(root)
    paths = ProjectPaths.discover(root)
    paths.ensure_output_dirs()
    catalog = AssetCatalog(paths)
    _admin_layers(paths)
    _hydro_layers(paths)
    _exposure_layers(paths)
    _raster_layers(paths, catalog)
    _other_raw_assets(paths, catalog)
    _event_layer(paths, broken=broken)
    return paths
