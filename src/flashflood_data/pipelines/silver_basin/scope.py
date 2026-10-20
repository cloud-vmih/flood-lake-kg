"""Select the Sơn La basin IDs represented by the canonical hydrological AOI."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pyogrio


@dataclass(frozen=True)
class BasinScope:
    basin_ids: tuple[str, ...]
    aoi_checksum: str


def read_basin_scope(root: Path, aoi_path: Path, *, level: int) -> BasinScope:
    """Recover exact basin membership from the AOI union, excluding boundary touches."""
    if level != 12:
        raise ValueError("Silver basin scope requires HydroBASINS level 12")
    path = root / aoi_path
    if not path.is_file():
        raise FileNotFoundError(path)
    layer = gpd.read_parquet(path)
    if layer.empty or layer.crs is None or layer.geometry.is_empty.any():
        raise ValueError("Silver hydrological AOI is empty or missing CRS")
    aoi = layer.geometry.union_all()
    source = root / "dataset" / "hybas_as_lev01-12_v1c" / "hybas_as_lev12_v1c.shp"
    if not source.is_file():
        raise FileNotFoundError(source)
    source_crs = pyogrio.read_info(source).get("crs")
    if not source_crs:
        raise ValueError("HydroBASINS source is missing CRS")
    source_aoi = gpd.GeoSeries([aoi], crs=layer.crs).to_crs(source_crs).iloc[0]
    nearby = pyogrio.read_dataframe(source, bbox=source_aoi.bounds, columns=["HYBAS_ID"])
    nearby = nearby.to_crs(layer.crs)
    selected = nearby.loc[nearby.geometry.representative_point().covered_by(aoi)]
    ids = tuple(sorted({str(int(value)) for value in selected["HYBAS_ID"]}, key=int))
    if not ids:
        raise ValueError("Silver hydrological AOI contains no basin interiors")
    if len(ids) != len(selected):
        raise ValueError("Silver hydrological AOI contains duplicate HYBAS_ID")
    return BasinScope(
        basin_ids=ids,
        aoi_checksum=hashlib.sha256(path.read_bytes()).hexdigest(),
    )
