"""Construction and persistence of the canonical study-area geometries."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
from shapely.geometry.base import BaseGeometry

from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.vector import write_geoparquet


@dataclass(frozen=True)
class StudyAreas:
    """Canonical AOIs, stored as geometries in the configured storage CRS."""

    core: BaseGeometry
    hydrological: BaseGeometry
    environmental: BaseGeometry
    exposure: BaseGeometry
    vietnam: BaseGeometry


def _project_geometry(geometry: BaseGeometry, source_crs: str, target_crs: str) -> BaseGeometry:
    return gpd.GeoSeries([geometry], crs=source_crs).to_crs(target_crs).iloc[0]


def build_study_areas(
    core: BaseGeometry,
    selected_l10: gpd.GeoDataFrame,
    vietnam: BaseGeometry,
    config: StudyAreaConfig,
) -> StudyAreas:
    """Build the four approved AOIs with all metric buffering done in processing CRS."""
    if selected_l10.empty:
        raise ValueError("cannot construct study areas from an empty L10 selection")
    if selected_l10.crs is None:
        raise ValueError("selected L10 basins must have a CRS")

    source_crs = selected_l10.crs.to_string()
    core_metric = _project_geometry(core, source_crs, config.processing_crs)
    vietnam_metric = _project_geometry(vietnam, source_crs, config.processing_crs)
    selected_metric = selected_l10.to_crs(config.processing_crs)
    hydrological_metric = selected_metric.geometry.union_all()
    environmental_metric = hydrological_metric.buffer(config.raster_buffer_km * 1_000)
    exposure_metric = core_metric.buffer(config.exposure_buffer_km * 1_000).intersection(vietnam_metric)

    return StudyAreas(
        core=_project_geometry(core_metric, config.processing_crs, config.storage_crs),
        hydrological=_project_geometry(hydrological_metric, config.processing_crs, config.storage_crs),
        environmental=_project_geometry(environmental_metric, config.processing_crs, config.storage_crs),
        exposure=_project_geometry(exposure_metric, config.processing_crs, config.storage_crs),
        vietnam=_project_geometry(vietnam_metric, config.processing_crs, config.storage_crs),
    )


def write_study_areas(
    areas: StudyAreas,
    output_dir: Path,
    storage_crs: str = "EPSG:4326",
    *,
    include_core: bool = True,
) -> list[Path]:
    """Persist one deterministic GeoParquet file for every canonical AOI."""
    layers = {
        "core_aoi": areas.core,
        "hydrological_aoi": areas.hydrological,
        "environmental_aoi": areas.environmental,
        "exposure_aoi": areas.exposure,
    }
    paths: list[Path] = []
    for name, geometry in layers.items():
        if name == "core_aoi" and not include_core:
            continue
        path = output_dir / f"{name}.geoparquet"
        layer = gpd.GeoDataFrame({"aoi": [name]}, geometry=[geometry], crs=storage_crs)
        paths.append(write_geoparquet(layer, path, storage_crs))
    return paths
