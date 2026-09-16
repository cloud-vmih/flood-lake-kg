"""Traceable SoilGrids scaling and thickness-weighted basin predictors."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio

from flashflood_data.static.features.config import load_feature_config
from flashflood_data.static.features.spatial import checked_basins, clipped_raster_values


def _depth_limits(label: str) -> tuple[int, int]:
    try:
        start_end = label.removesuffix("cm").split("-")
        return int(start_end[0]), int(start_end[1])
    except (IndexError, ValueError) as error:
        raise ValueError(f"invalid SoilGrids depth label: {label}") from error


def weighted_depth_value(values: Mapping[str, float], start_cm: int, end_cm: int) -> float:
    """Return an exact thickness-weighted value over a requested depth interval."""
    if end_cm <= start_cm:
        raise ValueError("depth interval must have positive thickness")
    numerator = 0.0
    overlap_total = 0
    for label, value in values.items():
        layer_start, layer_end = _depth_limits(label)
        overlap = max(0, min(end_cm, layer_end) - max(start_cm, layer_start))
        if overlap:
            if not np.isfinite(value):
                return float("nan")
            numerator += value * overlap
            overlap_total += overlap
    if overlap_total != end_cm - start_cm:
        return float("nan")
    return numerator / overlap_total


def depth_weighted_soil(
    raster_paths: Mapping[tuple[str, str, str], Path],
    basins: gpd.GeoDataFrame,
    bands_cm: Sequence[tuple[int, int]],
) -> pd.DataFrame:
    """Scale integer SoilGrids values then aggregate fully covered requested horizons."""
    config = load_feature_config()
    if tuple(bands_cm) != config.soil_depth_bands_cm:
        raise ValueError("SoilGrids derivation requires both configured depth bands")
    expected = {
        (property_name, depth, statistic)
        for property_name in config.soil_properties
        for depth in config.soil_depths
        for statistic in config.soil_statistics
    }
    provided = set(raster_paths)
    if provided != expected:
        raise ValueError("SoilGrids derivation requires a complete 96-asset property/depth/statistic matrix")
    selected = checked_basins(basins)
    grouped: dict[tuple[str, str], dict[str, Path]] = defaultdict(dict)
    for (property_name, depth, statistic), path in raster_paths.items():
        if property_name not in config.soil_divisors:
            raise ValueError(f"unsupported SoilGrids property: {property_name}")
        _depth_limits(depth)
        if statistic not in config.soil_statistics:
            raise ValueError(f"unsupported SoilGrids statistic: {statistic}")
        grouped[(property_name, statistic)][depth] = path

    rows: list[dict[str, float | int]] = [{"HYBAS_ID": int(identifier)} for identifier in selected.HYBAS_ID]
    for (property_name, statistic), paths_by_depth in grouped.items():
        per_depth: dict[str, list[tuple[float, int, int]]] = {}
        for depth, path in paths_by_depth.items():
            values_for_basins: list[tuple[float, int, int]] = []
            with rasterio.open(path) as dataset:
                for basin in selected.itertuples(index=False):
                    values, covered, valid = clipped_raster_values(dataset, basin.geometry, selected.crs)
                    mean = float(np.mean(values) / config.soil_divisors[property_name]) if valid else float("nan")
                    values_for_basins.append((mean, covered, valid))
            per_depth[depth] = values_for_basins
        for start_cm, end_cm in bands_cm:
            name = f"{property_name}_{statistic}_{start_cm}_{end_cm}cm"
            for index, row in enumerate(rows):
                values = {depth: metrics[index][0] for depth, metrics in per_depth.items()}
                coverage = [
                    (metrics[index][1], metrics[index][2])
                    for depth, metrics in per_depth.items()
                    if max(start_cm, _depth_limits(depth)[0]) < min(end_cm, _depth_limits(depth)[1])
                ]
                row[name] = weighted_depth_value(values, start_cm, end_cm)
                row[f"{name}_covered_pixel_count"] = sum(item[0] for item in coverage)
                row[f"{name}_valid_pixel_count"] = sum(item[1] for item in coverage)
                total_covered = row[f"{name}_covered_pixel_count"]
                row[f"{name}_coverage_fraction"] = (
                    row[f"{name}_valid_pixel_count"] / total_covered if total_covered else 0.0
                )
    return pd.DataFrame(rows)
