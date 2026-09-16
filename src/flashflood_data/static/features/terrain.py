"""Terrain predictors calculated on a 30 m metric DEM working grid."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.features import geometry_mask, geometry_window
from rasterio.vrt import WarpedVRT
from rasterio.warp import calculate_default_transform
from shapely.geometry import mapping

from flashflood_data.static.features.config import load_feature_config
from flashflood_data.static.features.spatial import checked_basins, geometry_in_dataset_crs
from flashflood_data.storage.atomic import atomic_target


def _finite_summary(values: np.ndarray) -> tuple[float, float, float, float]:
    if values.size == 0:
        return (float("nan"),) * 4
    return (
        float(np.min(values)),
        float(np.mean(values)),
        float(np.max(values)),
        float(np.percentile(values, 90)),
    )


def _metric_vrt(source: rasterio.io.DatasetReader, processing_crs: str) -> WarpedVRT:
    config = load_feature_config()
    if processing_crs != config.processing_crs:
        raise ValueError(f"Task 15 terrain processing CRS must be {config.processing_crs}")
    if source.crs is None:
        raise ValueError("DEM must have a CRS")
    transform, width, height = calculate_default_transform(
        source.crs,
        processing_crs,
        source.width,
        source.height,
        *source.bounds,
        resolution=config.terrain_resolution_m,
    )
    return WarpedVRT(
        source,
        crs=processing_crs,
        transform=transform,
        width=width,
        height=height,
        resampling=Resampling.bilinear,
    )


def _dataset_root(path: Path) -> Path | None:
    """Find the project dataset directory without assuming a working directory."""
    resolved = path.resolve()
    for candidate in (resolved.parent, *resolved.parents):
        if candidate.name == "dataset":
            return candidate
    return None


def _retain_metric_dem(dem: WarpedVRT, source_path: Path) -> Path | None:
    """Persist the metric 30 m input grid beside other derived assets, never raw inputs."""
    dataset_root = _dataset_root(source_path)
    if dataset_root is None:
        return None
    destination = dataset_root / "derived" / "terrain" / f"{source_path.stem}_metric_30m.tif"
    if destination.exists():
        return destination
    profile = dem.profile.copy()
    profile.update(driver="GTiff", compress="DEFLATE")
    with atomic_target(destination) as partial, rasterio.open(partial, "w", **profile) as output:
        for _, window in dem.block_windows(1):
            output.write(dem.read(1, window=window), 1, window=window)
            output.write_mask(dem.read_masks(1, window=window), window=window)
    return destination


def derive_terrain_features(
    dem_path: Path, basins: gpd.GeoDataFrame, processing_crs: str
) -> pd.DataFrame:
    """Calculate elevation and slope predictors, retaining one row per selected L10."""
    selected = checked_basins(basins)
    records: list[dict[str, float | int]] = []
    with rasterio.open(dem_path) as source, _metric_vrt(source, processing_crs) as dem:
        _retain_metric_dem(dem, dem_path)
        for basin in selected.itertuples(index=False):
            geometry = geometry_in_dataset_crs(basin.geometry, selected.crs, dem.crs)
            try:
                window = geometry_window(dem, [mapping(geometry)]).round_offsets().round_lengths()
                window = window.intersection(rasterio.windows.Window(0, 0, dem.width, dem.height))
            except rasterio.errors.WindowError:
                window = None
            if window is None or window.width <= 0 or window.height <= 0:
                elevation = np.array([], dtype="float64")
                slope = np.array([], dtype="float64")
                covered = valid = 0
            else:
                data = dem.read(1, window=window).astype("float64")
                source_valid = dem.read_masks(1, window=window) > 0
                inside = geometry_mask(
                    [mapping(geometry)],
                    out_shape=data.shape,
                    transform=dem.window_transform(window),
                    invert=True,
                )
                valid_mask = inside & source_valid & np.isfinite(data)
                covered = int(inside.sum())
                valid = int(valid_mask.sum())
                elevation = data[valid_mask]
                data[~source_valid | ~np.isfinite(data)] = np.nan
                if min(data.shape) < 2:
                    slope = np.array([], dtype="float64")
                else:
                    y_gradient, x_gradient = np.gradient(
                        data, abs(dem.transform.e), abs(dem.transform.a)
                    )
                    slope_grid = np.degrees(np.arctan(np.hypot(x_gradient, y_gradient)))
                    slope = slope_grid[valid_mask & np.isfinite(slope_grid)]
            elevation_min, elevation_mean, elevation_max, _ = _finite_summary(elevation)
            _, slope_mean, slope_max, slope_p90 = _finite_summary(slope)
            records.append(
                {
                    "HYBAS_ID": int(basin.HYBAS_ID),
                    "elevation_min_m": elevation_min,
                    "elevation_mean_m": elevation_mean,
                    "elevation_max_m": elevation_max,
                    "elevation_relief_m": elevation_max - elevation_min
                    if np.isfinite(elevation_min) and np.isfinite(elevation_max)
                    else float("nan"),
                    "slope_deg_mean": slope_mean,
                    "slope_deg_p90": slope_p90,
                    "slope_deg_max": slope_max,
                    "dem_covered_pixel_count": covered,
                    "dem_valid_pixel_count": valid,
                    "dem_coverage_fraction": valid / covered if covered else 0.0,
                }
            )
    return pd.DataFrame(records)
