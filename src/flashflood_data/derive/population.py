"""Native-grid, Core-AOI-only WorldPop evidence by selected L10 basin."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import shapely
from rasterio.windows import Window
from shapely.geometry.base import BaseGeometry

from flashflood_data.derive._spatial import checked_basins, geometry_in_dataset_crs, raster_windows


def _pixel_points(dataset: rasterio.io.DatasetReader, window: Window) -> tuple[np.ndarray, np.ndarray]:
    rows, columns = np.indices((int(window.height), int(window.width)))
    rows = rows + int(window.row_off)
    columns = columns + int(window.col_off)
    transform = dataset.transform
    return (
        transform.c + (columns + 0.5) * transform.a + (rows + 0.5) * transform.b,
        transform.f + (columns + 0.5) * transform.d + (rows + 0.5) * transform.e,
    )


def _assign_points_to_zones(
    points: np.ndarray, zones: dict[int, BaseGeometry]
) -> tuple[np.ndarray, np.ndarray]:
    """Assign point centers in one vectorized STRtree query; lowest ID wins ties."""
    winners = np.full(len(points), -1, dtype="int64")
    match_counts = np.zeros(len(points), dtype="int32")
    usable = [(identifier, geometry) for identifier, geometry in zones.items() if not geometry.is_empty]
    if not usable or not len(points):
        return winners, match_counts
    identifiers = np.asarray([item[0] for item in usable], dtype="int64")
    tree = shapely.STRtree([item[1] for item in usable])
    pairs = tree.query(points, predicate="covered_by")
    if pairs.size == 0:
        return winners, match_counts
    point_indexes, zone_indexes = pairs
    np.add.at(match_counts, point_indexes, 1)
    candidates = np.full(len(points), np.iinfo(np.int64).max, dtype="int64")
    np.minimum.at(candidates, point_indexes, identifiers[zone_indexes])
    matched = match_counts > 0
    winners[matched] = candidates[matched]
    return winners, match_counts


def aggregate_population_by_basin(
    worldpop: Path, basins: gpd.GeoDataFrame, core: BaseGeometry
) -> pd.DataFrame:
    """Assign every Core-AOI pixel center to at most one clipped L10 zone.

    WorldPop values are person counts, so values are neither resampled nor area weighted.
    A center exactly on two clipped zones is assigned to the lowest numeric HYBAS_ID;
    its winning row records the tie count as evidence.
    """
    selected = checked_basins(basins)
    if core.is_empty:
        raise ValueError("Core AOI must not be empty")
    totals = {
        int(identifier): {"sum": 0.0, "valid": 0, "nodata": 0, "aoi": 0, "ties": 0}
        for identifier in selected.HYBAS_ID
    }
    with rasterio.open(worldpop) as dataset:
        if dataset.count != 1 or dataset.crs is None:
            raise ValueError("WorldPop input must be a single-band raster with a CRS")
        core_in_raster = geometry_in_dataset_crs(core, selected.crs, dataset.crs)
        zones = {
            int(row.HYBAS_ID): geometry_in_dataset_crs(row.geometry, selected.crs, dataset.crs).intersection(core_in_raster)
            for row in selected.itertuples(index=False)
        }
        for window in raster_windows(dataset):
            values = dataset.read(1, window=window)
            valid_mask = dataset.read_masks(1, window=window) > 0
            x, y = _pixel_points(dataset, window)
            points = shapely.points(x.ravel(), y.ravel())
            in_core = (shapely.contains(core_in_raster, points) | shapely.touches(core_in_raster, points))
            if not in_core.any():
                continue
            flattened = values.ravel()
            flattened_valid = valid_mask.ravel() & np.isfinite(flattened)
            core_offsets = np.flatnonzero(in_core)
            winners, match_counts = _assign_points_to_zones(points[core_offsets], zones)
            for identifier, record in totals.items():
                assigned = winners == identifier
                assigned_count = int(assigned.sum())
                if not assigned_count:
                    continue
                offsets = core_offsets[assigned]
                assigned_valid = flattened_valid[offsets]
                record["aoi"] += assigned_count
                record["ties"] += int((match_counts[assigned] > 1).sum())
                record["sum"] += float(flattened[offsets[assigned_valid]].sum())
                record["valid"] += int(assigned_valid.sum())
                record["nodata"] += int((~assigned_valid).sum())
        resolution_x = float(np.hypot(dataset.transform.a, dataset.transform.d))
        resolution_y = float(np.hypot(dataset.transform.b, dataset.transform.e))
        resolution_unit = "degree" if dataset.crs.is_geographic else str(dataset.crs.linear_units)
        source_crs = dataset.crs.to_string()
    rows: list[dict[str, object]] = []
    for identifier in selected.HYBAS_ID:
        record = totals[int(identifier)]
        valid = int(record["valid"])
        aoi = int(record["aoi"])
        rows.append(
            {
                "HYBAS_ID": int(identifier),
                "population_sum": float(record["sum"]),
                "population_mean": float(record["sum"]) / aoi if aoi else float("nan"),
                "contributing_pixel_count": valid,
                "nodata_pixel_count": int(record["nodata"]),
                "aoi_pixel_count": aoi,
                "coverage_ratio": valid / aoi if aoi else float("nan"),
                "source_resolution_x": resolution_x,
                "source_resolution_y": resolution_y,
                "source_resolution_unit": resolution_unit,
                "source_crs": source_crs,
                "population_scope": "core_aoi_only",
                "boundary_center_tie_pixel_count": int(record["ties"]),
                "quality_flags_json": json.dumps(
                    {
                        "boundary_center_tie_pixel_count": int(record["ties"]),
                        "constrained_nodata_population_policy": "zero_during_aggregation",
                    },
                    sort_keys=True,
                ),
                "source_asset_ids_json": "[]",
            }
        )
    return pd.DataFrame(rows).sort_values("HYBAS_ID", kind="stable").reset_index(drop=True)


def map_subbasin_population(worldpop: Path, basins: gpd.GeoDataFrame, core: BaseGeometry) -> pd.DataFrame:
    """Compatibility name for the WorldPop relationship product."""
    return aggregate_population_by_basin(worldpop, basins, core)
