"""Movable, display-only MapLibre inspection bundle generation."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from PIL import Image
from rasterio.warp import transform_bounds
from shapely.geometry import mapping

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.io_atomic import atomic_target

_VECTOR_LAYERS: dict[str, tuple[tuple[str, ...], float]] = {
    "communes": (("harmonized/admin/admin_commune_2025.geoparquet",), 20.0),
    "subbasins_l10": (("harmonized/hydro/subbasin_l10.geoparquet",), 20.0),
    "rivers": (("harmonized/hydro/river_reach.geoparquet",), 10.0),
    "roads": (
        ("derived/exposure/road_segment.geoparquet", "harmonized/exposure/road_segment.geoparquet"),
        5.0,
    ),
    "bridges": (
        ("derived/exposure/bridge.geoparquet", "harmonized/exposure/bridge.geoparquet"),
        5.0,
    ),
    "facilities": (
        ("derived/exposure/facility.geoparquet", "harmonized/exposure/facility.geoparquet"),
        5.0,
    ),
    "settlements": (
        ("derived/exposure/settlement.geoparquet", "harmonized/exposure/settlement.geoparquet"),
        5.0,
    ),
}
_RASTERS = {
    "dem": "harmonized/rasters/dem_glo30.tif",
    "worldcover": "harmonized/rasters/worldcover_2021.tif",
    "worldpop": "harmonized/rasters/worldpop_2025.tif",
}
QA_MAP_BUNDLE_RELATIVE_PATHS = (
    Path("map/data/bridges.geojson"),
    Path("map/data/communes.geojson"),
    Path("map/data/facilities.geojson"),
    Path("map/data/rivers.geojson"),
    Path("map/data/roads.geojson"),
    Path("map/data/settlements.geojson"),
    Path("map/data/subbasins_l10.geojson"),
    Path("map/previews/dem.png"),
    Path("map/previews/worldcover.png"),
    Path("map/previews/worldpop.png"),
    Path("map/layer-manifest.json"),
    Path("map/index.html"),
)
_MAPPING_DEFINITIONS = {
    "communes": ("commune", ("current_commune_code",)),
    "subbasins_l10": ("population", ("HYBAS_ID",)),
    "rivers": ("river", ("HYRIV_ID",)),
    "roads": ("road", ("segment_id", "osm_id")),
    "bridges": ("bridge", ("osm_id",)),
    "facilities": ("facility", ("osm_id",)),
    "settlements": ("settlement", ("osm_id",)),
}
_MAPPING_EVIDENCE_COLUMNS = frozenset(
    {
        "HYBAS_ID",
        "intersection_area_km2",
        "basin_fraction",
        "commune_fraction",
        "intersected_length_km",
        "relationship_geometry_wkt",
        "relationship_type",
        "tags_json",
        "boundary_case",
        "boundary_center_tie_pixel_count",
        "population_sum",
        "population_mean",
        "contributing_pixel_count",
        "nodata_pixel_count",
        "aoi_pixel_count",
        "coverage_ratio",
        "source_resolution_x",
        "source_resolution_y",
        "source_resolution_unit",
        "source_crs",
        "population_scope",
        "quality_flags_json",
        "processing_crs",
        "source_asset_ids_json",
    }
)


def _evidence_truthy(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, Mapping):
        return any(_evidence_truthy(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_evidence_truthy(item) for item in value)
    if isinstance(value, (float, np.floating)):
        return bool(np.isfinite(value) and value != 0)
    if isinstance(value, (int, np.integer, bool, np.bool_)):
        return bool(value)
    text = str(value).strip()
    return text not in {"", "0", "False", "false", "None", "none", "null"}


def _quality_warning(value: object) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if text in {"", "{}", "[]", "null", "None", "nan"}:
        return False
    try:
        return _evidence_truthy(json.loads(text))
    except (TypeError, ValueError, json.JSONDecodeError):
        return True


def _geojson(
    path: Path,
    output: Path,
    tolerance_m: float,
    mapping_warnings: Mapping[str, set[str]] | None = None,
) -> dict[str, object]:
    try:
        layer = gpd.read_parquet(path)
        if layer.crs is None:
            raise ValueError("missing CRS")
        metric = layer.to_crs("EPSG:32648").copy()
        metric.geometry = metric.geometry.simplify(tolerance_m, preserve_topology=True)
        display = metric.to_crs("EPSG:4326")
        properties = [column for column in display.columns if column != "geometry"]
        features: list[dict[str, object]] = []
        for _, row in display.iterrows():
            values = {column: _json_value(row[column]) for column in properties}
            values["qa_warning"] = (
                _quality_warning(values.get("quality_flags_json"))
                or _evidence_truthy(values.get("geometry_repaired", False))
                or _evidence_truthy(values.get("boundary_case", False))
                or any(
                    str(values.get(identifier, "")) in warned_ids
                    for identifier, warned_ids in (mapping_warnings or {}).items()
                )
            )
            features.append(
                {
                    "type": "Feature",
                    "properties": dict(sorted(values.items())),
                    "geometry": mapping(row.geometry) if row.geometry is not None else None,
                }
            )
        features.sort(
            key=lambda feature: json.dumps(feature["properties"], sort_keys=True, default=str)
        )
        payload: dict[str, object] = {"type": "FeatureCollection", "features": features}
        bounds = [float(value) for value in display.total_bounds] if not display.empty else None
        present = True
    except Exception:  # noqa: BLE001 - missing source is represented in the manifest.
        payload, bounds, present = {"type": "FeatureCollection", "features": []}, None, False
    with atomic_target(output) as partial:
        partial.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
    return {
        "type": "vector",
        "path": f"data/{output.name}",
        "bounds": bounds,
        "present": present,
        "tolerance_m": tolerance_m,
    }


def _mapping_warning_lookup(
    paths: ProjectPaths, name: str, source_path: Path
) -> dict[str, set[str]]:
    definition = _MAPPING_DEFINITIONS.get(name)
    if definition is None:
        return {}
    mapping_name, preferred_identifiers = definition
    path = next(
        (
            candidate
            for candidate in (
                paths.derived / "mappings" / f"map_subbasin_{mapping_name}.parquet",
                paths.derived / f"map_subbasin_{mapping_name}.parquet",
            )
            if candidate.is_file()
        ),
        None,
    )
    if path is None or not source_path.is_file():
        return {}
    try:
        table = pd.read_parquet(path)
        source = gpd.read_parquet(source_path)
        shared = set(table.columns) & set(source.columns)
        identifier = next(
            (candidate for candidate in preferred_identifiers if candidate in shared), None
        )
        if identifier is None:
            candidates = [
                column
                for column in shared - _MAPPING_EVIDENCE_COLUMNS - {"geometry"}
                if table[column].notna().all()
                and source[column].notna().all()
                and source[column].is_unique
            ]
            candidates.sort(
                key=lambda column: (
                    not str(column).casefold().endswith(("_id", "_code", "_key")),
                    str(column),
                )
            )
            identifier = candidates[0] if candidates else None
        if identifier is None:
            return {}
        warned = table.get("quality_flags_json", pd.Series("{}", index=table.index)).map(
            _quality_warning
        )
        warned |= table.get("boundary_case", pd.Series(False, index=table.index)).map(
            _evidence_truthy
        )
        tie_counts = pd.to_numeric(
            table.get("boundary_center_tie_pixel_count", pd.Series(0, index=table.index)),
            errors="coerce",
        )
        warned |= tie_counts.fillna(0).gt(0)
        return {identifier: set(table.loc[warned, identifier].dropna().astype(str))}
    except Exception:  # noqa: BLE001 - map remains inspectable if an optional mapping is malformed.
        return {}


def _json_value(value: Any) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def _preview(path: Path | None, output: Path) -> dict[str, object]:
    bounds: list[float] | None = None
    present = False
    if path is not None and path.is_file():
        try:
            with rasterio.open(path) as dataset:
                height, width = min(512, dataset.height), min(512, dataset.width)
                image = dataset.read(1, out_shape=(height, width), masked=True).astype("float64")
                valid = image.compressed()
                if valid.size:
                    low, high = np.percentile(valid, [2, 98])
                    scaled = (
                        np.zeros(image.shape, dtype="uint8")
                        if high <= low
                        else np.clip((image.filled(low) - low) / (high - low) * 255, 0, 255).astype(
                            "uint8"
                        )
                    )
                else:
                    scaled = np.zeros(image.shape, dtype="uint8")
                Image.fromarray(scaled, mode="L").save(output, format="PNG", optimize=False)
                raw_bounds = dataset.bounds
                bounds = (
                    list(
                        transform_bounds(
                            dataset.crs,
                            "EPSG:4326",
                            raw_bounds.left,
                            raw_bounds.bottom,
                            raw_bounds.right,
                            raw_bounds.top,
                            densify_pts=21,
                        )
                    )
                    if dataset.crs
                    else None
                )
                present = True
        except Exception:  # noqa: BLE001
            present = False
    if not present:
        Image.new("L", (1, 1), 0).save(output, format="PNG", optimize=False)
    return {
        "type": "raster-preview",
        "path": f"previews/{output.name}",
        "bounds": bounds,
        "present": present,
    }


def publish_qa_map(paths: ProjectPaths, qa_dir: Path) -> Path:
    """Publish a relocatable MapLibre map without modifying analytical sources."""
    bundle = qa_dir / "map"
    data_dir, preview_dir = bundle / "data", bundle / "previews"
    data_dir.mkdir(parents=True, exist_ok=True)
    preview_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, object] = {"layers": {}}
    for name, (candidates, tolerance) in sorted(_VECTOR_LAYERS.items()):
        source = next(
            (
                paths.dataset / candidate
                for candidate in candidates
                if (paths.dataset / candidate).is_file()
            ),
            paths.dataset / candidates[0],
        )
        manifest["layers"][name] = _geojson(
            source,
            data_dir / f"{name}.geojson",
            tolerance,
            _mapping_warning_lookup(paths, name, source),
        )  # type: ignore[index]
    for name, relative in sorted(_RASTERS.items()):
        source = paths.dataset / relative
        manifest["layers"][name] = _preview(
            source if source.is_file() else None, preview_dir / f"{name}.png"
        )  # type: ignore[index]
    manifest_path = bundle / "layer-manifest.json"
    with atomic_target(manifest_path) as partial:
        partial.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    index = bundle / "index.html"
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    template = Environment(
        loader=FileSystemLoader(Path(__file__).with_name("templates")),
        autoescape=select_autoescape(("html",)),
    ).get_template("map.html.j2")
    with atomic_target(index) as partial:
        partial.write_text(template.render(), encoding="utf-8")
    return index
