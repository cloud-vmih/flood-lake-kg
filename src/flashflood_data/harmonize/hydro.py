"""HydroBASINS topology and local hydrology harmonization."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry

from flashflood_data.aoi import StudyAreas, write_study_areas
from flashflood_data.io_atomic import atomic_target
from flashflood_data.paths import ProjectPaths
from flashflood_data.vector import write_geoparquet


@dataclass(frozen=True)
class HydroInputPaths:
    """Explicit source vectors used by :func:`harmonize_hydro`."""

    l10: Path
    l9: Path
    l8: Path
    basinatlas_l10: Path
    hydrorivers: Path


def _ids(frame: gpd.GeoDataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        raise ValueError(f"hydro layer is missing required {column} column")
    return pd.to_numeric(frame[column], errors="raise").astype("int64")


def select_l10_with_upstream(
    l10: gpd.GeoDataFrame, core: BaseGeometry, hops: int = 1
) -> gpd.GeoDataFrame:
    """Select basins intersecting Core plus an exact number of upstream topology hops."""
    if hops < 0:
        raise ValueError("upstream hops must be non-negative")
    if l10.crs is None:
        raise ValueError("L10 basins must have a CRS")
    basin_ids = _ids(l10, "HYBAS_ID")
    if basin_ids.duplicated().any():
        raise ValueError("L10 layer contains duplicate HYBAS_ID values")
    downstream = _ids(l10, "NEXT_DOWN")
    direct = set(basin_ids.loc[l10.geometry.intersects(core)])
    selected = set(direct)
    frontier = direct
    for _ in range(hops):
        if not frontier:
            break
        upstream = set(basin_ids.loc[downstream.isin(frontier)])
        selected.update(upstream)
        frontier = upstream
    result = l10.loc[basin_ids.isin(selected)].copy()
    return result.assign(HYBAS_ID=_ids(result, "HYBAS_ID")).sort_values("HYBAS_ID").reset_index(drop=True)


def _parent_for_point(
    point: BaseGeometry, parents: gpd.GeoDataFrame, level: int, child_id: int
) -> pd.Series:
    candidates = parents.loc[parents.geometry.map(lambda geometry: geometry.covers(point))]
    if len(candidates) != 1:
        raise ValueError(
            f"L10 basin {child_id} has {len(candidates)} containing level-{level} parents; expected one"
        )
    return candidates.iloc[0]


def _pfaf(value: object) -> str:
    text = str(value).strip()
    return text.removesuffix(".0")


def build_basin_hierarchy(
    l10: gpd.GeoDataFrame, l9: gpd.GeoDataFrame, l8: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Assign each L10 basin exactly one spatial L9/L8 parent and validate Pfafstetter nesting."""
    if l10.crs is None or l9.crs is None or l8.crs is None:
        raise ValueError("all basin layers must have a CRS")
    for layer in (l10, l9, l8):
        for field in ("HYBAS_ID", "PFAF_ID"):
            if field not in layer.columns:
                raise ValueError(f"hydro layer is missing required {field} column")
    l9_local = l9.to_crs(l10.crs)
    l8_local = l8.to_crs(l10.crs)
    l10_ids = _ids(l10, "HYBAS_ID")
    selected_ids = set(l10_ids)
    downstream = _ids(l10, "NEXT_DOWN")
    rows: list[dict[str, int | bool]] = []
    for index, child in l10.iterrows():
        child_id = int(l10_ids.loc[index])
        point = child.geometry.representative_point()
        parent_l9 = _parent_for_point(point, l9_local, 9, child_id)
        parent_l8 = _parent_for_point(point, l8_local, 8, child_id)
        child_pfaf = _pfaf(child["PFAF_ID"])
        if not child_pfaf.startswith(_pfaf(parent_l9["PFAF_ID"])):
            raise ValueError(f"L10 basin {child_id} Pfafstetter ID is not nested in its L9 parent")
        if not child_pfaf.startswith(_pfaf(parent_l8["PFAF_ID"])):
            raise ValueError(f"L10 basin {child_id} Pfafstetter ID is not nested in its L8 parent")
        next_down = int(downstream.loc[index])
        rows.append(
            {
                "HYBAS_ID": child_id,
                "parent_l9_hybas_id": int(parent_l9["HYBAS_ID"]),
                "parent_l8_hybas_id": int(parent_l8["HYBAS_ID"]),
                "scope_exit": next_down != 0 and next_down not in selected_ids,
            }
        )
    return pd.DataFrame(rows).sort_values("HYBAS_ID").reset_index(drop=True)


def default_hydro_inputs(paths: ProjectPaths) -> HydroInputPaths:
    """Locate only the standard HydroBASINS and HydroATLAS source layers."""
    return HydroInputPaths(
        l10=paths.dataset / "hybas_as_lev01-12_v1c" / "hybas_as_lev10_v1c.shp",
        l9=paths.dataset / "hybas_as_lev01-12_v1c" / "hybas_as_lev09_v1c.shp",
        l8=paths.dataset / "hybas_as_lev01-12_v1c" / "hybas_as_lev08_v1c.shp",
        basinatlas_l10=(
            paths.dataset / "BasinATLAS_Data_v10_shp" / "BasinATLAS_v10_shp" / "BasinATLAS_v10_lev10.shp"
        ),
        hydrorivers=(
            paths.dataset / "HydroRIVERS_v10_as_shp" / "HydroRIVERS_v10_as_shp" / "HydroRIVERS_v10_as.shp"
        ),
    )


def _write_parquet(frame: pd.DataFrame, path: Path) -> Path:
    with atomic_target(path) as partial:
        frame.to_parquet(partial, index=False)
    return path


def harmonize_hydro(
    paths: ProjectPaths, study_areas: StudyAreas, *, inputs: HydroInputPaths | None = None, hops: int = 1
) -> list[Path]:
    """Write selected L10, validated parents, and clipped existing hydro layers."""
    source = inputs or default_hydro_inputs(paths)
    l10 = gpd.read_file(source.l10)
    selected = select_l10_with_upstream(l10, study_areas.core, hops=hops)
    hierarchy = build_basin_hierarchy(selected, gpd.read_file(source.l9), gpd.read_file(source.l8))

    hydro_dir = paths.harmonized / "hydro"
    aoi_dir = paths.harmonized / "aoi"
    aoi_paths = write_study_areas(
        study_areas,
        aoi_dir,
        include_core=not (aoi_dir / "core_aoi.geoparquet").is_file(),
    )
    selected_path = write_geoparquet(selected, hydro_dir / "subbasin_l10.geoparquet")
    hierarchy_path = _write_parquet(hierarchy, hydro_dir / "subbasin_hierarchy.parquet")

    selected_ids = set(_ids(selected, "HYBAS_ID"))
    atlas = gpd.read_file(source.basinatlas_l10)
    atlas_subset = atlas.loc[_ids(atlas, "HYBAS_ID").isin(selected_ids)].copy()
    atlas_subset = atlas_subset.sort_values("HYBAS_ID").reset_index(drop=True)
    atlas_path = write_geoparquet(atlas_subset, hydro_dir / "basinatlas_l10.geoparquet")

    rivers = gpd.read_file(source.hydrorivers).to_crs(selected.crs)
    river_subset = rivers.loc[rivers.geometry.intersects(study_areas.hydrological)].copy()
    river_subset["geometry"] = river_subset.geometry.intersection(study_areas.hydrological)
    river_subset = river_subset.loc[~river_subset.geometry.is_empty].reset_index(drop=True)
    river_path = write_geoparquet(river_subset, hydro_dir / "river_reach.geoparquet")
    return [*aoi_paths, selected_path, hierarchy_path, atlas_path, river_path]
