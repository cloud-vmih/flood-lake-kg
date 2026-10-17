"""Native-grid WorldPop harmonization used by the existing-file adapter."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import rasterio
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.windows import Window
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

from flashflood_data.shared.geospatial.raster import COG_PROFILE
from flashflood_data.storage.atomic import atomic_target


def harmonize_worldpop(raw_path: Path, core_aoi: BaseGeometry, output: Path) -> Path:
    """Exact-clip a WorldPop raster while retaining its native grid and cell values."""
    if core_aoi.is_empty:
        raise ValueError("Core AOI must not be empty")
    with rasterio.open(raw_path) as source:
        if source.count != 1 or source.crs is None:
            raise ValueError("WorldPop input must be a single-band raster with a CRS")
        dtype = source.dtypes[0]
        if not np.issubdtype(np.dtype(dtype), np.number):
            raise ValueError("WorldPop input must have a numeric cell type")
        try:
            crop = geometry_window(source, [mapping(core_aoi)]).round_offsets().round_lengths()
            crop = crop.intersection(Window(0, 0, source.width, source.height))
        except WindowError as error:
            raise ValueError("Core AOI does not intersect WorldPop input") from error
        if crop.width <= 0 or crop.height <= 0:
            raise ValueError("Core AOI does not intersect WorldPop input")
        profile = source.profile.copy()
        profile.update(
            COG_PROFILE,
            dtype="float32",
            nodata=float(source.nodata) if source.nodata is not None else None,
            width=int(crop.width),
            height=int(crop.height),
            transform=source.window_transform(crop),
            predictor=3,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        with atomic_target(output) as partial, rasterio.open(partial, "w", **profile) as destination:
            values = source.read(1, window=crop, out_dtype="float32")
            valid = source.read_masks(1, window=crop) > 0
            inside = geometry_mask(
                [mapping(core_aoi)],
                out_shape=(int(crop.height), int(crop.width)),
                transform=source.window_transform(crop),
                invert=True,
            )
            valid &= inside
            if source.nodata is not None:
                values[~valid] = np.float32(source.nodata)
            destination.write(values, 1)
            destination.write_mask(np.where(valid, 255, 0).astype("uint8"))
            factors = [
                factor
                for factor in (2, 4, 8, 16)
                if min(destination.width, destination.height) >= factor
            ]
            if factors:
                destination.build_overviews(factors, rasterio.enums.Resampling.average)
                destination.update_tags(ns="rio_overview", resampling="average")
    return output
