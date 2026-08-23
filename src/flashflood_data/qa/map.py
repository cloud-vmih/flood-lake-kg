"""Movable, display-only MapLibre inspection bundle generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import rasterio
from PIL import Image
from rasterio.warp import transform_bounds
from shapely.geometry import mapping

from flashflood_data.io_atomic import atomic_target
from flashflood_data.paths import ProjectPaths

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


def _geojson(path: Path, output: Path, tolerance_m: float) -> dict[str, object]:
    try:
        layer = gpd.read_parquet(path)
        if layer.crs is None:
            raise ValueError("missing CRS")
        metric = layer.to_crs("EPSG:3857").copy()
        metric.geometry = metric.geometry.simplify(tolerance_m, preserve_topology=True)
        display = metric.to_crs("EPSG:4326")
        properties = [column for column in display.columns if column != "geometry"]
        features: list[dict[str, object]] = []
        for _, row in display.iterrows():
            values = {column: _json_value(row[column]) for column in properties}
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
                paths.root / candidate
                for candidate in candidates
                if (paths.root / candidate).is_file()
            ),
            paths.root / candidates[0],
        )
        manifest["layers"][name] = _geojson(source, data_dir / f"{name}.geojson", tolerance)  # type: ignore[index]
    for name, relative in sorted(_RASTERS.items()):
        source = paths.root / relative
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
