"""Native-grid WorldCover class fractions with explicit nodata evidence."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.windows import Window
from shapely.geometry import mapping

from flashflood_data.derive._spatial import checked_basins, geometry_in_dataset_crs

WORLDCOVER_CLASSES = {
    10: "tree_cover",
    20: "shrubland",
    30: "grassland",
    40: "cropland",
    50: "built_up",
    60: "bare_sparse",
    70: "snow_ice",
    80: "permanent_water",
    90: "herbaceous_wetland",
    95: "mangroves",
    100: "moss_lichen",
}


def derive_landcover_fractions(worldcover_path: Path, basins: gpd.GeoDataFrame) -> pd.DataFrame:
    """Compute categorical fractions on WorldCover's native grid (no resampling)."""
    selected = checked_basins(basins)
    rows: list[dict[str, float | int]] = []
    with rasterio.open(worldcover_path) as dataset:
        if dataset.crs is None:
            raise ValueError("WorldCover raster must have a CRS")
        for basin in selected.itertuples(index=False):
            geometry = geometry_in_dataset_crs(basin.geometry, selected.crs, dataset.crs)
            try:
                window = geometry_window(dataset, [mapping(geometry)]).round_offsets().round_lengths()
                window = window.intersection(Window(0, 0, dataset.width, dataset.height))
            except WindowError:
                window = None
            counts = {name: 0 for name in WORLDCOVER_CLASSES.values()}
            if window is None or window.width <= 0 or window.height <= 0:
                covered = source_valid = recognized = 0
            else:
                data = dataset.read(1, window=window)
                inside = geometry_mask(
                    [mapping(geometry)],
                    out_shape=data.shape,
                    transform=dataset.window_transform(window),
                    invert=True,
                )
                valid_mask = inside & (dataset.read_masks(1, window=window) > 0)
                covered = int(inside.sum())
                source_valid = int(valid_mask.sum())
                for code, name in WORLDCOVER_CLASSES.items():
                    counts[name] = int((valid_mask & (data == code)).sum())
                recognized = sum(counts.values())
            row: dict[str, float | int] = {
                "HYBAS_ID": int(basin.HYBAS_ID),
                "landcover_covered_pixel_count": covered,
                "landcover_valid_pixel_count": recognized,
                "landcover_nodata_pixel_count": covered - source_valid,
                "landcover_unknown_pixel_count": source_valid - recognized,
                "landcover_coverage_fraction": recognized / covered if covered else 0.0,
            }
            for name, count in counts.items():
                row[f"landcover_fraction_{name}"] = count / recognized if recognized else 0.0
            rows.append(row)
    return pd.DataFrame(rows)
