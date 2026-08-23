"""Metric HydroRIVERS and BasinATLAS static basin predictors."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.warp import transform
from shapely.geometry import Point

from flashflood_data.derive._spatial import checked_basins
from flashflood_data.derive.features import load_feature_config

PROCESSING_CRS = "EPSG:32648"
def _sample_endpoint(dataset: rasterio.io.DatasetReader, point: Point) -> float | None:
    if dataset.crs is None:
        raise ValueError("DEM must have a CRS")
    x, y = transform(PROCESSING_CRS, dataset.crs, [point.x], [point.y])
    value = next(dataset.sample([(x[0], y[0])], masked=True))[0]
    if np.ma.is_masked(value) or not np.isfinite(value):
        return None
    return float(value)


def _basinatlas_values(basinatlas: gpd.GeoDataFrame | None) -> pd.DataFrame | None:
    if basinatlas is None:
        return None
    fields = load_feature_config().basinatlas_fields
    required = {"HYBAS_ID", *fields}
    missing = sorted(required - set(basinatlas.columns))
    if missing:
        raise ValueError(f"BasinATLAS is missing required fields: {', '.join(missing)}")
    atlas = checked_basins(basinatlas)
    values = pd.DataFrame(atlas[["HYBAS_ID", *fields]])
    numeric = values.loc[:, fields].apply(pd.to_numeric, errors="raise")
    if not np.isfinite(numeric.to_numpy(dtype="float64")).all():
        raise ValueError("nonfinite BasinATLAS values are not allowed")
    return values


def derive_hydrology_features(
    rivers: gpd.GeoDataFrame,
    basins: gpd.GeoDataFrame,
    dem_path: Path | None = None,
    basinatlas: gpd.GeoDataFrame | None = None,
    *,
    dem: Path | None = None,
) -> pd.DataFrame:
    """Intersect rivers in EPSG:32648 and optionally add sampled stream gradients."""
    if dem_path is not None and dem is not None:
        raise ValueError("provide only one of dem_path or dem")
    selected = checked_basins(basins).to_crs(PROCESSING_CRS)
    if rivers.crs is None:
        raise ValueError("rivers must have a CRS")
    metric_rivers = rivers.to_crs(PROCESSING_CRS)
    atlas = _basinatlas_values(basinatlas)
    rows: list[dict[str, float | int]] = []
    dem_source = dem_path if dem_path is not None else dem
    with rasterio.open(dem_source) if dem_source is not None else _NoDem() as dataset:
        for basin in selected.itertuples(index=False):
            clipped = metric_rivers.geometry.intersection(basin.geometry)
            clipped = clipped.loc[clipped.notna() & ~clipped.is_empty]
            lengths = clipped.length
            total_length_m = float(lengths.sum())
            area_km2 = float(basin.geometry.area / 1_000_000)
            gradients: list[tuple[float, float]] = []
            reversed_count = flat_count = nodata_count = 0
            if dataset is not None:
                for reach in clipped:
                    if reach.geom_type != "LineString" or reach.length == 0:
                        nodata_count += 1
                        continue
                    start = _sample_endpoint(dataset, Point(reach.coords[0]))
                    end = _sample_endpoint(dataset, Point(reach.coords[-1]))
                    if start is None or end is None:
                        nodata_count += 1
                    elif end > start:
                        reversed_count += 1
                        gradients.append((abs(end - start) / reach.length, reach.length))
                    elif end == start:
                        flat_count += 1
                        gradients.append((0.0, reach.length))
                    else:
                        gradients.append(((start - end) / reach.length, reach.length))
            length_weight = sum(length for _, length in gradients)
            rows.append(
                {
                    "HYBAS_ID": int(basin.HYBAS_ID),
                    "basin_area_km2": area_km2,
                    "river_length_km": total_length_m / 1_000,
                    "river_reach_count": len(clipped),
                    "drainage_density_km_per_km2": total_length_m / 1_000 / area_km2
                    if area_km2 > 0
                    else float("nan"),
                    "stream_gradient_m_per_m": sum(value * length for value, length in gradients)
                    / length_weight
                    if length_weight
                    else float("nan"),
                    "stream_gradient_valid_reach_count": len(gradients),
                    "stream_gradient_reversed_reach_count": reversed_count,
                    "stream_gradient_flat_reach_count": flat_count,
                    "stream_gradient_nodata_reach_count": nodata_count,
                }
            )
    result = pd.DataFrame(rows)
    if atlas is not None:
        result = result.merge(atlas, on="HYBAS_ID", how="left", validate="one_to_one")
    return result


class _NoDem:
    """Context manager that presents no raster when stream-gradient sampling is disabled."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_: object) -> None:
        return None
