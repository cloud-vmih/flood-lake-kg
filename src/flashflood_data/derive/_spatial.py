"""Small, bounded raster/vector helpers for static predictor derivation."""

from __future__ import annotations

from collections.abc import Iterator

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.warp import transform_geom
from rasterio.windows import Window
from shapely.geometry import mapping, shape
from shapely.geometry.base import BaseGeometry


def checked_basins(basins: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Return an ID-normalized basin copy, rejecting ambiguous feature keys."""
    if basins.crs is None:
        raise ValueError("basins must have a CRS")
    if "HYBAS_ID" not in basins.columns:
        raise ValueError("basins are missing required HYBAS_ID column")
    ids = pd.to_numeric(basins["HYBAS_ID"], errors="raise")
    numeric = ids.to_numpy(dtype="float64")
    if not np.isfinite(numeric).all():
        raise ValueError("HYBAS_ID values must be finite")
    if ids.duplicated().any():
        raise ValueError("duplicate HYBAS_ID values are not allowed")
    result = basins.copy()
    result["HYBAS_ID"] = ids.astype("int64")
    return result.sort_values("HYBAS_ID").reset_index(drop=True)


def geometry_in_dataset_crs(geometry: BaseGeometry, source_crs: object, dataset_crs: object) -> BaseGeometry:
    """Reproject one basin geometry without changing its source GeoDataFrame."""
    if str(source_crs) == str(dataset_crs):
        return geometry
    return shape(transform_geom(source_crs, dataset_crs, mapping(geometry)))


def clipped_raster_values(
    dataset: rasterio.io.DatasetReader,
    geometry: BaseGeometry,
    source_crs: object,
) -> tuple[np.ndarray, int, int]:
    """Read just a basin window and return finite values, covered, and valid pixels."""
    if dataset.crs is None:
        raise ValueError("raster input must have a CRS")
    target = geometry_in_dataset_crs(geometry, source_crs, dataset.crs)
    try:
        window = geometry_window(dataset, [mapping(target)]).round_offsets().round_lengths()
    except WindowError:
        return np.array([], dtype="float64"), 0, 0
    full = Window(0, 0, dataset.width, dataset.height)
    window = window.intersection(full)
    if window.width <= 0 or window.height <= 0:
        return np.array([], dtype="float64"), 0, 0
    inside = geometry_mask(
        [mapping(target)],
        out_shape=(int(window.height), int(window.width)),
        transform=dataset.window_transform(window),
        invert=True,
    )
    data = dataset.read(1, window=window)
    valid_mask = dataset.read_masks(1, window=window) > 0
    finite = np.isfinite(data)
    valid = inside & valid_mask & finite
    return data[valid].astype("float64", copy=False), int(inside.sum()), int(valid.sum())


def raster_windows(dataset: rasterio.io.DatasetReader, block_size: int = 512) -> Iterator[Window]:
    """Yield fixed-size raster windows when callers need streaming processing."""
    for row_off in range(0, dataset.height, block_size):
        for col_off in range(0, dataset.width, block_size):
            yield Window(
                col_off,
                row_off,
                min(block_size, dataset.width - col_off),
                min(block_size, dataset.height - row_off),
            )
