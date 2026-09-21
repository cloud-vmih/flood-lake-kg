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
_BASINATLAS_FIELDS = (
    "HYBAS_ID", "NEXT_DOWN", "NEXT_SINK", "MAIN_BAS", "DIST_SINK", "DIST_MAIN",
    "SUB_AREA", "UP_AREA", "PFAF_ID", "SORT", "ele_mt_sav", "ele_mt_smn", "ele_mt_smx",
    "slp_dg_sav", "sgr_dk_sav", "lka_pc_sse", "dor_pc_pva", "rev_mc_usu",
    "for_pc_sse", "crp_pc_sse", "glc_pc_s22", "wet_pc_sg1", "wet_pc_sg2",
    "inu_pc_slt", "gwt_cm_sav", "run_mm_syr", "dis_m3_pyr", "dis_m3_pmx",
    "pop_ct_ssu", "ppd_pk_sav",
)


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


def _feature_id(source_id: str, attributes: dict[str, object], position: int) -> str:
    preferred = _VECTOR_SOURCES[source_id][1]
    candidates = (preferred,) if preferred else (
        "MaXa", "ma_xa", "Mã xã", "a02_xa", "id", "GID_3", "GID_2", "GID_1", "GID_0",
    )
    for name in candidates:
        value = attributes.get(name) if name else None
        if value is not None and str(value).strip():
            if isinstance(value, (int, float, np.integer, np.floating)) and float(value).is_integer():
                return str(int(value))
            return str(value)
    if preferred:
        raise ValueError(f"{source_id} lacks its source feature ID: {preferred}")
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
        source_id, path, object_id=object_id, run_id=run_id,
        parser_version=parser_version,
    )
    rows = [row for batch in batches for row in batch]
    identities = [str(row["source_feature_id"]) for row in rows]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate source feature ID")
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
            selected_columns = [name for name in _BASINATLAS_FIELDS if name in available]
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
                    "EPSG:4326", "OGC:CRS84", "+init=epsg:4326"
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
                    if geometry is None or geometry.is_empty:
                        raise ValueError(f"invalid raw geometry at feature {position}")
                    if table_name in {"basin_polygon_raw", "admin_boundary_raw"} and geometry.geom_type not in {
                        "Polygon", "MultiPolygon"
                    }:
                        raise ValueError("polygon source contains a non-polygon feature")
                    if table_name == "river_reach_raw" and geometry.geom_type not in {
                        "LineString", "MultiLineString"
                    }:
                        raise ValueError("river source contains a non-line feature")
                    feature_id = _feature_id(source_id, attributes, position)
                    raw_bbox = list(map(float, wgs84_geom.bounds))
                    bbox = _bbox_wgs84(wgs84_geom)
                    row = {
                        "object_id": object_id,
                        "source_feature_id": feature_id,
                        "source_fields_json": _json(attributes),
                        "geometry_wkb": geometry.wkb,
                        "crs": crs,
                        "bbox_wgs84": bbox,
                        "ingest_run_id": run_id,
                        "parser_version": parser_version,
                        "quality_status": "passed" if geometry.is_valid and bbox == raw_bbox else "flagged",
                    }
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
    return property_name, depth.group(0) if depth else None, statistic.group(0) if statistic else None


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


def parse_events(
    path: Path, *, object_id: str, run_id: str, parser_version: str
) -> list[dict[str, object]]:
    """Preserve original workbook/CSV values and date text before Silver cleanup."""
    if path.suffix.lower() in {".xlsx", ".xls"}:
        frame = pd.read_excel(path, dtype=object)
    elif path.suffix.lower() == ".csv":
        frame = pd.read_csv(path, dtype=object)
    else:
        raise ValueError(f"unsupported structured event file: {path.suffix}")
    if frame.empty:
        raise ValueError("event source contains no records")
    rows = []
    keys: set[str] = set()
    for position, (_, source) in enumerate(frame.iterrows(), start=1):
        attributes = {name: source[name] for name in frame.columns}
        original_id = _native(attributes.get("STT"))
        record_id = str(int(original_id)) if isinstance(original_id, (int, float)) and float(original_id).is_integer() else f"row-{position}"
        if record_id in keys:
            raise ValueError(f"duplicate historical event source ID: {record_id}")
        keys.add(record_id)
        text = _native(attributes.get("Mô tả ngắn"))
        valid_time = _native(attributes.get("Ngày xảy ra"))
        rows.append({
            "object_id": object_id,
            "source_record_id": record_id,
            "source_document_id": None,
            "event_text": None if text is None else str(text),
            "original_fields_json": _json(attributes),
            "source_valid_time": None if valid_time is None else str(valid_time),
            "ingest_run_id": run_id,
            "parser_version": parser_version,
            "quality_status": "passed",
        })
    return rows
