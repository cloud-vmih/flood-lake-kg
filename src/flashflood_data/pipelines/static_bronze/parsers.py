"""Source-faithful Bronze parsers; mapping and feature derivation happen later."""

import json
import re
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile, is_zipfile

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import rasterio
from rasterio.warp import transform_bounds
from shapely import make_valid

from flashflood_data.pipelines.static_landing.adapters.hydro_fields import BASINATLAS_RAW_FIELDS

_VECTOR_SOURCES = {
    "hydrobasins_v1c": ("basin_polygon_raw", "HYBAS_ID"),
    "basinatlas_v10": ("basin_polygon_raw", "HYBAS_ID"),
    "hydrorivers_v10": ("river_reach_raw", "HYRIV_ID"),
    "sonla_admin_2025": ("admin_boundary_raw", None),
    "gadm_vnm_4_1": ("admin_boundary_raw", None),
}
_RASTER_PROPERTIES = {
    "cop_dem_glo30_2024_1": "elevation",
    "esa_worldcover_2021_v200": "landcover",
    "worldpop_vnm_2025": "population",
}
_SOIL_PROPERTIES = ("clay", "sand", "silt", "bdod", "cfvo", "soc", "wv0033", "wv1500")
_BOUND_EDGE_TOLERANCE_DEGREES = 1e-3


def _native(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (np.integer, np.floating)):
        value = value.item()
    if isinstance(value, (int, float, str, bool)):
        return None if isinstance(value, float) and np.isnan(value) else value
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_native(item) for item in value]
    return str(value)


def _json(value: dict[str, object]) -> str:
    return json.dumps(
        {str(name): _native(item) for name, item in value.items()},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _bbox_wgs84(geometry: object) -> list[float]:
    """Clamp projection roundoff at valid world bounds without hiding bad coordinates."""
    values = list(map(float, geometry.bounds))
    limits = ((-180.0, 180.0), (-90.0, 90.0), (-180.0, 180.0), (-90.0, 90.0))
    for index, (lower, upper) in enumerate(limits):
        if lower - _BOUND_EDGE_TOLERANCE_DEGREES <= values[index] < lower:
            values[index] = lower
        elif upper < values[index] <= upper + _BOUND_EDGE_TOLERANCE_DEGREES:
            values[index] = upper
    return values


def _shapefile_members(archive: Path) -> list[str]:
    with ZipFile(archive) as bundle:
        names = bundle.namelist()
    shape_files = [name for name in names if name.lower().endswith(".shp")]
    if not shape_files:
        raise ValueError("source bundle contains no shapefile")
    for shp in shape_files:
        stem = shp[:-4]
        for suffix in (".shp", ".shx", ".dbf", ".prj"):
            if not any(name.lower() == f"{stem}{suffix}".lower() for name in names):
                raise ValueError(f"source bundle is missing {suffix} member for {shp}")
    return sorted(shape_files)


def _shapefile_member(archive: Path) -> str:
    members = _shapefile_members(archive)
    if len(members) != 1:
        raise ValueError("source bundle must contain exactly one shapefile")
    return members[0]


def _feature_id(source_id: str, attributes: dict[str, object], position: int) -> str | None:
    preferred = _VECTOR_SOURCES[source_id][1]
    candidates = (
        (preferred,)
        if preferred
        else (
            "MaXa",
            "ma_xa",
            "Mã xã",
            "a02_xa",
            "id",
            "GID_3",
            "GID_2",
            "GID_1",
            "GID_0",
        )
    )
    for name in candidates:
        value = attributes.get(name) if name else None
        if value is not None and not bool(pd.isna(value)) and str(value).strip():
            if (
                isinstance(value, (int, float, np.integer, np.floating))
                and float(value).is_integer()
            ):
                return str(int(value))
            return str(value)
    if preferred:
        return None
    return f"row-{position}"


def parse_vector(
    source_id: str,
    path: Path,
    *,
    object_id: str,
    run_id: str,
    parser_version: str,
) -> tuple[str, list[dict[str, object]]]:
    """Convenience reader for small fixtures; production uses bounded batches."""
    table_name, _ = _VECTOR_SOURCES[source_id]
    batches = iter_vector_batches(
        source_id,
        path,
        object_id=object_id,
        run_id=run_id,
        parser_version=parser_version,
    )
    rows = [row for batch in batches for row in batch]
    return table_name, rows


def iter_vector_batches(
    source_id: str,
    path: Path,
    *,
    object_id: str,
    run_id: str,
    parser_version: str,
    batch_size: int = 5_000,
) -> Iterator[list[dict[str, object]]]:
    """Read one source layer window at a time while checking object-wide IDs."""
    if batch_size < 1:
        raise ValueError("vector batch_size must be positive")
    table_name, _ = _VECTOR_SOURCES[source_id]
    if path.suffix.lower() == ".zip":
        members = _shapefile_members(path)
        layer_paths = [f"/vsizip/{path.resolve()}/{member}" for member in members]
    else:
        with path.open("rb") as f:
            magic = f.read(2)
        if magic == b"\x1f\x8b":
            layer_paths = [f"/vsigzip/{path.resolve()}"]
        else:
            layer_paths = [str(path)]

    position = 0

    for layer_path in layer_paths:
        layer_read = False
        selected_columns = None
        if source_id == "basinatlas_v10":
            available = set(pyogrio.read_info(layer_path)["fields"])
            selected_columns = [name for name in BASINATLAS_RAW_FIELDS if name in available]
        with pyogrio.open_arrow(
            layer_path, use_pyarrow=True, batch_size=batch_size, columns=selected_columns
        ) as (_, reader):
            for arrow_batch in reader:
                layer_read = True
                layer = gpd.GeoDataFrame.from_arrow(arrow_batch)
                if layer.crs is None:
                    raise ValueError("raw vector has no declared CRS")
                crs = layer.crs.to_string()
                is_wgs84 = layer.crs.to_epsg() == 4326 or crs in (
                    "EPSG:4326",
                    "OGC:CRS84",
                    "+init=epsg:4326",
                )
                geographic = layer if is_wgs84 else layer.to_crs("EPSG:4326")
                geom_name = layer.geometry.name
                attr_names = [name for name in layer.columns if name != geom_name]
                records = layer[attr_names].to_dict(orient="records")
                rows: list[dict[str, object]] = []
                for geometry, wgs84_geom, attributes in zip(
                    layer.geometry.values, geographic.geometry.values, records, strict=True
                ):
                    position += 1
                    feature_id = _feature_id(source_id, attributes, position)
                    warnings: list[str] = []
                    if geometry is not None and not geometry.is_empty and not geometry.is_valid:
                        repaired = make_valid(geometry)
                        repaired_wgs84 = make_valid(wgs84_geom)
                        expected_types = (
                            {"Polygon", "MultiPolygon"}
                            if table_name in {"basin_polygon_raw", "admin_boundary_raw"}
                            else {"LineString", "MultiLineString"}
                            if table_name == "river_reach_raw"
                            else None
                        )
                        if (
                            not repaired.is_empty
                            and repaired.is_valid
                            and (expected_types is None or repaired.geom_type in expected_types)
                            and not repaired_wgs84.is_empty
                            and repaired_wgs84.is_valid
                        ):
                            geometry = repaired
                            wgs84_geom = repaired_wgs84
                            warnings.append("geometry_repaired")
                    raw_bbox = (
                        None
                        if wgs84_geom is None or wgs84_geom.is_empty
                        else list(map(float, wgs84_geom.bounds))
                    )
                    bbox = None if raw_bbox is None else _bbox_wgs84(wgs84_geom)
                    if bbox is not None and bbox != raw_bbox:
                        warnings.append("bbox_clamped")
                    row = {
                        "object_id": object_id,
                        "source_feature_id": feature_id,
                        "source_fields_json": _json(attributes),
                        "geometry_wkb": (
                            None if geometry is None or geometry.is_empty else geometry.wkb
                        ),
                        "crs": crs,
                        "bbox_wgs84": bbox,
                        "ingest_run_id": run_id,
                        "parser_version": parser_version,
                        "quality_status": "warning" if warnings else "passed",
                    }
                    if feature_id is None:
                        row["_dq_source_record_key"] = f"{source_id}:row-{position}"
                    if warnings:
                        row["_dq_warning_codes"] = tuple(warnings)
                    if table_name in {"basin_polygon_raw", "admin_boundary_raw"}:
                        row["source_id"] = source_id
                    rows.append(row)
                yield rows
        if not layer_read:
            raise ValueError("raw vector contains no features")


def _raster_labels(source_id: str, name: str) -> tuple[str | None, str | None, str | None]:
    if source_id != "soilgrids_2_0":
        return _RASTER_PROPERTIES.get(source_id), None, None
    tokens = set(re.split(r"[^a-z0-9]+", name.lower()))
    property_name = next((part for part in _SOIL_PROPERTIES if part in tokens), None)
    depth = re.search(r"\d+-\d+cm", name, re.IGNORECASE)
    statistic = re.search(r"Q0\.(?:05|50|95|5)|mean", name, re.IGNORECASE)
    return (
        property_name,
        depth.group(0) if depth else None,
        statistic.group(0) if statistic else None,
    )


def parse_raster(
    path: Path,
    *,
    object_id: str,
    object_uri: str,
    checksum: str,
    run_id: str,
    parser_version: str,
    source_id: str,
) -> list[dict[str, object]]:
    """Read only raster headers and one record per band; never materialize pixels."""
    property_name, depth, statistic = _raster_labels(source_id, path.name)
    raster_target = path
    temp_dir_ctx: TemporaryDirectory | None = None
    if source_id == "cop_dem_glo30_2024_1" and (path.suffix.lower() == ".dem" or is_zipfile(path)):
        with ZipFile(path) as bundle:
            dem_member = next(m for m in bundle.namelist() if m.lower().endswith("_dem.tif"))
            temp_dir_ctx = TemporaryDirectory(prefix="dem-extract-")
            extracted = bundle.extract(dem_member, path=temp_dir_ctx.name)
            raster_target = Path(extracted)
    try:
        with rasterio.open(raster_target) as raster:
            if raster.crs is None or raster.count < 1:
                raise ValueError("raw raster must declare CRS and at least one band")
            bbox = list(map(float, transform_bounds(raster.crs, "EPSG:4326", *raster.bounds)))
            return [
                {
                    "object_id": object_id,
                    "band_or_layer": f"band-{band}",
                    "property": property_name,
                    "depth_interval": depth,
                    "statistic": statistic,
                    "object_uri": object_uri,
                    "crs": raster.crs.to_string(),
                    "bbox_wgs84": bbox,
                    "resolution_x": float(abs(raster.res[0])),
                    "resolution_y": float(abs(raster.res[1])),
                    "nodata": None if raster.nodata is None else float(raster.nodata),
                    "dtype": raster.dtypes[band - 1],
                    "checksum": checksum,
                    "ingest_run_id": run_id,
                    "parser_version": parser_version,
                    "quality_status": "passed",
                }
                for band in range(1, raster.count + 1)
            ]
    finally:
        if temp_dir_ctx is not None:
            temp_dir_ctx.cleanup()
