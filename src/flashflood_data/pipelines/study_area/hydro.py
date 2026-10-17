"""HydroBASINS selection used to construct local and national AOIs."""

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry


def _ids(frame: gpd.GeoDataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        raise ValueError(f"hydro layer is missing required {column} column")
    return pd.to_numeric(frame[column], errors="raise").astype("int64")


def select_basins_with_upstream(
    basins: gpd.GeoDataFrame, core: BaseGeometry, hops: int = 1
) -> gpd.GeoDataFrame:
    """Select intersecting basins plus a bounded number of upstream topology hops."""
    if hops < 0:
        raise ValueError("upstream hops must be non-negative")
    if basins.crs is None:
        raise ValueError("basins must have a CRS")
    basin_ids = _ids(basins, "HYBAS_ID")
    if basin_ids.duplicated().any():
        raise ValueError("basin layer contains duplicate HYBAS_ID values")
    downstream = _ids(basins, "NEXT_DOWN")
    direct = set(basin_ids.loc[basins.geometry.intersects(core)])
    selected = set(direct)
    frontier = direct
    for _ in range(hops):
        if not frontier:
            break
        upstream = set(basin_ids.loc[downstream.isin(frontier)])
        selected.update(upstream)
        frontier = upstream
    result = basins.loc[basin_ids.isin(selected)].copy()
    return (
        result.assign(HYBAS_ID=_ids(result, "HYBAS_ID"))
        .sort_values("HYBAS_ID")
        .reset_index(drop=True)
    )
