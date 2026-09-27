"""Deterministic national subsets of large HydroSHEDS/HydroATLAS vectors."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio

from flashflood_data.catalog import sha256_file
from flashflood_data.static.sources.hydro_fields import BASINATLAS_RAW_FIELDS

_FEATURE_IDS = {
    "hydrobasins_v1c": "HYBAS_ID",
    "basinatlas_v10": "HYBAS_ID",
    "hydrorivers_v10": "HYRIV_ID",
}
_OUTPUT_NAMES = {
    "hydrobasins_v1c": "hydrobasins_l12_vietnam_h1.shp",
    "basinatlas_v10": "basinatlas_l12_vietnam_h1.shp",
    "hydrorivers_v10": "hydrorivers_vietnam_h1.shp",
}


@dataclass(frozen=True)
class HydroSubsetResult:
    path: Path
    source_feature_count: int
    selected_feature_count: int
    selected_hybas_ids: tuple[int, ...]
    aoi_checksum: str
    selection_version: str = "vietnam-l12-h1-v1"


def _aoi(path: Path) -> gpd.GeoDataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    layer = gpd.read_parquet(path)
    if layer.empty or layer.crs is None or layer.geometry.is_empty.any():
        raise ValueError("national hydrological AOI is empty or has no CRS")
    return layer


def _source_columns(source_id: str, source_path: Path) -> list[str] | None:
    if source_id != "basinatlas_v10":
        return None
    fields = tuple(map(str, pyogrio.read_info(source_path)["fields"]))
    by_lower = {field.lower(): field for field in fields}
    selected = [by_lower[name.lower()] for name in BASINATLAS_RAW_FIELDS if name.lower() in by_lower]
    if "hybas_id" not in by_lower:
        raise ValueError("basinatlas_v10 is missing HYBAS_ID")
    return selected


def _integer_ids(frame: gpd.GeoDataFrame, name: str) -> pd.Series:
    if name not in frame.columns:
        raise ValueError(f"hydro source is missing {name}")
    values = pd.to_numeric(frame[name], errors="raise").astype("int64")
    if values.duplicated().any():
        raise ValueError(f"hydro source contains duplicate {name} values")
    return values


def build_hydro_subset(
    source_id: str,
    source_path: Path,
    aoi_path: Path,
    output_dir: Path,
    *,
    selected_hybas_ids: Collection[int] | None = None,
) -> HydroSubsetResult:
    """Write a complete-feature national subset ready for deterministic bundling."""
    if source_id not in _FEATURE_IDS:
        raise ValueError(f"unsupported hydro subset source: {source_id}")
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    info = pyogrio.read_info(source_path)
    source_crs = info.get("crs")
    if not source_crs:
        raise ValueError("hydro source has no CRS")
    national = _aoi(aoi_path).to_crs(source_crs)
    geometry = national.geometry.union_all()
    frame = pyogrio.read_dataframe(
        source_path,
        bbox=geometry.bounds,
        columns=_source_columns(source_id, source_path),
    )
    if frame.crs is None:
        raise ValueError("hydro source has no CRS")
    frame = frame.loc[frame.geometry.intersects(geometry)].copy()
    id_field = _FEATURE_IDS[source_id]
    ids = _integer_ids(frame, id_field)
    if source_id == "basinatlas_v10":
        if selected_hybas_ids is None:
            raise ValueError("BasinATLAS subset requires selected HydroBASINS IDs")
        wanted = {int(value) for value in selected_hybas_ids}
        frame = frame.loc[ids.isin(wanted)].copy()
        ids = _integer_ids(frame, id_field)
        if set(ids) != wanted:
            missing = sorted(wanted - set(ids))
            raise ValueError(f"BasinATLAS is missing selected HYBAS_ID values: {missing}")
    if frame.empty:
        raise ValueError(f"{source_id} national subset contains no features")
    frame[id_field] = ids
    frame = frame.sort_values(id_field).reset_index(drop=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / _OUTPUT_NAMES[source_id]
    frame.to_file(target, driver="ESRI Shapefile", index=False)
    hybas_ids = (
        tuple(int(value) for value in frame["HYBAS_ID"])
        if "HYBAS_ID" in frame.columns
        else ()
    )
    return HydroSubsetResult(
        path=target,
        source_feature_count=int(info["features"]),
        selected_feature_count=len(frame),
        selected_hybas_ids=hybas_ids,
        aoi_checksum=sha256_file(aoi_path),
    )
