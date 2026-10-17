"""Pure field and geometry normalization for canonical Silver basin rows."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from functools import lru_cache
from hashlib import sha256

from pyproj import CRS, Transformer
from shapely import from_wkb, make_valid, normalize, to_wkb
from shapely.errors import GEOSException
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform, unary_union

from flashflood_data.pipelines.silver_basin.config import BasinTopologyConfig
from flashflood_data.pipelines.silver_basin.models import BasinRow
from flashflood_data.pipelines.silver_basin.reader import (
    BasinBronzeInputs,
    BasinBronzeRow,
)

_HYDRO_IDENTIFIER_FIELDS = (
    "basin_id",
    "next_down_id",
    "main_basin_id",
    "pfaf_id",
    "next_sink_id",
)
_ATLAS_METRIC_FIELDS = (
    "basin_area_km2",
    "upstream_area_km2",
    "distance_to_main_sink_km",
    "distance_to_sink_km",
)


def _identifier(value: object, field: str, *, optional: bool = False) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        if optional:
            return None
        raise ValueError(f"basin source field {field} is required")
    if isinstance(value, bool):
        raise TypeError(f"basin source field {field} must be an identifier")
    if isinstance(value, int):
        normalized = str(value)
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError(f"basin source field {field} must be an integer-like identifier")
        normalized = str(int(value))
    elif isinstance(value, str):
        normalized = value.strip()
    else:
        raise TypeError(f"basin source field {field} must be an identifier")
    if optional and normalized == "0":
        return None
    return normalized


def _integer(value: object, field: str) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool):
        raise TypeError(f"basin source field {field} must be an integer")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"basin source field {field} must be an integer") from error
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f"basin source field {field} must be an integer")
    return int(number)


def _number(value: object, field: str, *, optional: bool = False) -> float | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        if optional:
            return None
        raise ValueError(f"basin source field {field} is required")
    if isinstance(value, bool):
        raise TypeError(f"basin source field {field} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"basin source field {field} must be numeric") from error
    if not math.isfinite(number):
        if optional:
            return None
        raise ValueError(f"basin source field {field} must be finite")
    return number


def _mapped(
    fields: Mapping[str, object],
    config: BasinTopologyConfig,
    target: str,
) -> object:
    source_name = config.field_map[target]
    return fields.get(source_name)


def _polygon_parts(geometry: BaseGeometry) -> list[Polygon]:
    if isinstance(geometry, Polygon):
        return [geometry]
    if isinstance(geometry, MultiPolygon):
        return list(geometry.geoms)
    if isinstance(geometry, GeometryCollection):
        return [part for member in geometry.geoms for part in _polygon_parts(member)]
    return []


def _repair_polygon(geometry: BaseGeometry) -> Polygon | MultiPolygon:
    candidate = geometry if geometry.is_valid else make_valid(geometry)
    parts = _polygon_parts(candidate)
    if not parts:
        raise ValueError("HydroBASINS geometry is not a repairable polygon")
    polygon = unary_union(parts)
    if (
        not isinstance(polygon, (Polygon, MultiPolygon))
        or polygon.is_empty
        or not polygon.is_valid
    ):
        raise ValueError("HydroBASINS geometry is not a repairable polygon")
    return polygon


@lru_cache(maxsize=16)
def _transformer(source_crs: str, target_crs: str) -> Transformer:
    return Transformer.from_crs(
        CRS.from_user_input(source_crs),
        CRS.from_user_input(target_crs),
        always_xy=True,
    )


def _canonical_geometry(row: BasinBronzeRow, target_crs: str) -> tuple[bytes, str]:
    try:
        geometry = from_wkb(row.geometry_wkb)
    except GEOSException as error:
        raise ValueError(f"HydroBASINS {row.hybas_id} contains invalid WKB") from error
    if geometry is None or geometry.is_empty:
        raise ValueError(f"HydroBASINS {row.hybas_id} contains empty geometry")
    polygon = _repair_polygon(geometry)

    source = CRS.from_user_input(row.crs)
    target = CRS.from_user_input(target_crs)
    if source != target:
        polygon = _repair_polygon(
            transform(_transformer(source.to_string(), target.to_string()).transform, polygon)
        )

    canonical = normalize(polygon)
    payload = to_wkb(
        canonical,
        byte_order=1,
        include_srid=False,
        output_dimension=2,
    )
    return payload, sha256(payload).hexdigest()


def _atlas_by_id(
    inputs: BasinBronzeInputs,
    config: BasinTopologyConfig,
) -> dict[str, BasinBronzeRow]:
    indexed: dict[str, BasinBronzeRow] = {}
    for row in inputs.atlas_rows:
        if row.source_id != config.atlas_source_id:
            raise ValueError(f"unexpected BasinATLAS source: {row.source_id}")
        if row.hybas_id in indexed:
            raise ValueError(f"duplicate BasinATLAS HYBAS_ID: {row.hybas_id}")
        indexed[row.hybas_id] = row
    missing = sorted({row.hybas_id for row in inputs.hydro_rows} - indexed.keys())
    if missing:
        raise ValueError(f"BasinATLAS is missing HYBAS_ID values: {missing}")
    return indexed


def normalize_basins(
    inputs: BasinBronzeInputs,
    config: BasinTopologyConfig,
    basin_version: str,
    valid_from: datetime,
) -> list[BasinRow]:
    """Join approved Atlas metrics onto authoritative HydroBASINS L12 rows."""
    atlas_by_id = _atlas_by_id(inputs, config)
    normalized: list[BasinRow] = []
    for hydro in inputs.hydro_rows:
        if hydro.source_id != config.hydro_source_id:
            raise ValueError(f"unexpected HydroBASINS source: {hydro.source_id}")
        atlas = atlas_by_id[hydro.hybas_id]
        hydro_fields = hydro.source_fields
        atlas_fields = atlas.source_fields
        geometry_wkb, geometry_hash = _canonical_geometry(hydro, config.target_crs)

        identifiers = {
            target: _identifier(
                _mapped(hydro_fields, config, target),
                config.field_map[target],
                optional=target != "basin_id",
            )
            for target in _HYDRO_IDENTIFIER_FIELDS
        }
        metrics = {
            target: _number(
                _mapped(atlas_fields, config, target),
                config.field_map[target],
                optional=target.startswith("distance_"),
            )
            for target in _ATLAS_METRIC_FIELDS
        }
        normalized.append(
            BasinRow(
                basin_id=str(identifiers["basin_id"]),
                basin_version=basin_version,
                hydrobasins_level=config.hydrobasins_level,
                geometry_wkb=geometry_wkb,
                crs=config.target_crs,
                geometry_hash=geometry_hash,
                next_down_id=identifiers["next_down_id"],
                main_basin_id=identifiers["main_basin_id"],
                pfaf_id=identifiers["pfaf_id"],
                topology_sort_hint=_integer(
                    _mapped(hydro_fields, config, "topology_sort_hint"),
                    config.field_map["topology_sort_hint"],
                ),
                basin_area_km2=metrics["basin_area_km2"],
                upstream_area_km2=metrics["upstream_area_km2"],
                distance_to_main_sink_km=metrics["distance_to_main_sink_km"],
                next_sink_id=identifiers["next_sink_id"],
                distance_to_sink_km=metrics["distance_to_sink_km"],
                valid_from=valid_from,
                valid_to=None,
                source_object_id=hydro.object_id,
            )
        )
    return sorted(normalized, key=lambda row: (row.basin_id, row.source_object_id))
