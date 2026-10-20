"""Spark/Sedona normalization for canonical Silver basin rows.

This module only builds lazy DataFrame transformations. It never collects basin
rows into the driver and it never owns an Iceberg commit.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from flashflood_data.pipelines.silver_basin.config import BasinTopologyConfig

_FIELD_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10


def _field(name: str) -> str:
    if _FIELD_NAME.fullmatch(name) is None:
        raise ValueError(f"unsafe basin source field: {name}")
    return name


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_field(source_name: str) -> str:
    return f"get_json_object(source_fields_json, '$.{_field(source_name)}')"


def _optional_identifier(expression: str) -> str:
    return (
        f"CASE WHEN trim({expression}) IN ('', '0') THEN NULL "
        f"ELSE trim({expression}) END"
    )


def _boolean_flag(expression: str) -> str:
    return (
        f"CASE WHEN CAST({expression} AS INT) = 1 THEN TRUE "
        f"WHEN CAST({expression} AS INT) = 0 THEN FALSE ELSE NULL END"
    )


@dataclass(frozen=True)
class BasinNormalizationPlan:
    """Validated SQL projections used by the lazy basin transform."""

    hydro_source_filter: str
    atlas_source_filter: str
    hydro_source_projection: tuple[str, ...]
    hydro_geometry_projection: tuple[str, ...]
    atlas_projection: tuple[str, ...]
    output_projection: tuple[str, ...]
    join_key: str = "basin_id"


def basin_normalization_plan(
    config: BasinTopologyConfig,
    *,
    basin_version: str,
    valid_from: datetime,
) -> BasinNormalizationPlan:
    """Compile the approved basin mapping into deterministic Spark SQL expressions."""
    if not basin_version.strip():
        raise ValueError("basin_version must not be empty")
    if valid_from.tzinfo is None or valid_from.utcoffset() != _UTC.utcoffset(valid_from):
        raise ValueError("valid_from must use timezone-aware UTC")

    mapped = {target: _field(source) for target, source in config.field_map.items()}
    basin_id = _json_field(mapped["basin_id"])
    target_crs = _literal(config.target_crs)
    timestamp = valid_from.astimezone(_UTC).strftime("%Y-%m-%d %H:%M:%S.%f")

    # The geometry has one stable WKB representation before hashing.
    parsed_geometry = "ST_GeomFromWKB(source_geometry_wkb)"
    repaired_geometry = f"ST_MakeValid({parsed_geometry})"
    transformed_geometry = (
        f"CASE WHEN upper(source_crs) = {target_crs} THEN {repaired_geometry} "
        f"ELSE ST_Transform({repaired_geometry}, source_crs, {target_crs}) END"
    )
    canonical_geometry = f"ST_Normalize(ST_Force_2D({transformed_geometry}))"

    hydro_source_projection = (
        f"trim({basin_id}) AS basin_id",
        "object_id AS hydrobasins_object_id",
        "crs AS source_crs",
        "geometry_wkb AS source_geometry_wkb",
        (
            f"{_optional_identifier(_json_field(mapped['next_down_id']))} "
            f"AS hydro_{mapped['next_down_id']}"
        ),
        (
            f"{_optional_identifier(_json_field(mapped['main_basin_id']))} "
            f"AS hydro_{mapped['main_basin_id']}"
        ),
        (
            f"{_optional_identifier(_json_field(mapped['pfaf_id']))} "
            f"AS hydro_{mapped['pfaf_id']}"
        ),
        (
            f"CAST({_json_field(mapped['topology_sort_hint'])} AS BIGINT) "
            f"AS hydro_{mapped['topology_sort_hint']}"
        ),
        (
            f"CAST({_json_field(mapped['hydro_order'])} AS INT) "
            f"AS hydro_{mapped['hydro_order']}"
        ),
        (
            f"{_boolean_flag(_json_field(mapped['is_coastal']))} "
            f"AS hydro_{mapped['is_coastal']}"
        ),
        (
            f"{_boolean_flag(_json_field(mapped['is_endorheic']))} "
            f"AS hydro_{mapped['is_endorheic']}"
        ),
        (
            f"{_optional_identifier(_json_field(mapped['next_sink_id']))} "
            f"AS hydro_{mapped['next_sink_id']}"
        ),
    )
    hydro_geometry_projection = (
        "basin_id",
        "hydrobasins_object_id",
        f"ST_AsBinary({canonical_geometry}) AS geometry_wkb",
        f"NOT ST_IsValid({parsed_geometry}) AS _geometry_was_repaired",
        (
            f"array(CAST(ST_XMin({canonical_geometry}) AS DOUBLE), "
            f"CAST(ST_YMin({canonical_geometry}) AS DOUBLE), "
            f"CAST(ST_XMax({canonical_geometry}) AS DOUBLE), "
            f"CAST(ST_YMax({canonical_geometry}) AS DOUBLE)) AS bbox_wgs84"
        ),
        f"{target_crs} AS crs",
        f"hydro_{mapped['next_down_id']}",
        f"hydro_{mapped['main_basin_id']}",
        f"hydro_{mapped['pfaf_id']}",
        f"hydro_{mapped['topology_sort_hint']}",
        f"hydro_{mapped['hydro_order']}",
        f"hydro_{mapped['is_coastal']}",
        f"hydro_{mapped['is_endorheic']}",
        f"hydro_{mapped['next_sink_id']}",
    )
    atlas_projection = (
        f"trim({basin_id}) AS basin_id",
        "object_id AS basinatlas_object_id",
        (
            f"CAST({_json_field(mapped['basin_area_km2'])} AS DOUBLE) "
            "AS atlas_basin_area_km2"
        ),
        (
            f"CAST({_json_field(mapped['upstream_area_km2'])} AS DOUBLE) "
            "AS atlas_upstream_area_km2"
        ),
        (
            f"CAST({_json_field(mapped['distance_to_main_sink_km'])} AS DOUBLE) "
            "AS atlas_distance_to_main_sink_km"
        ),
        (
            f"CAST({_json_field(mapped['distance_to_sink_km'])} AS DOUBLE) "
            "AS atlas_distance_to_sink_km"
        ),
    )
    output_projection = (
        "h.basin_id AS basin_id",
        f"{_literal(basin_version.strip())} AS basin_version",
        f"CAST({config.hydrobasins_level} AS INT) AS hydrobasins_level",
        "h.geometry_wkb AS geometry_wkb",
        "h._geometry_was_repaired AS _geometry_was_repaired",
        "h.bbox_wgs84 AS bbox_wgs84",
        "h.crs AS crs",
        "sha2(h.geometry_wkb, 256) AS geometry_hash",
        f"h.hydro_{mapped['next_down_id']} AS next_down_id",
        f"h.hydro_{mapped['main_basin_id']} AS main_basin_id",
        f"h.hydro_{mapped['pfaf_id']} AS pfaf_id",
        f"h.hydro_{mapped['topology_sort_hint']} AS topology_sort_hint",
        f"h.hydro_{mapped['hydro_order']} AS hydro_order",
        f"h.hydro_{mapped['is_coastal']} AS is_coastal",
        f"h.hydro_{mapped['is_endorheic']} AS is_endorheic",
        "a.atlas_basin_area_km2 AS basin_area_km2",
        "a.atlas_upstream_area_km2 AS upstream_area_km2",
        "a.atlas_distance_to_main_sink_km AS distance_to_main_sink_km",
        f"h.hydro_{mapped['next_sink_id']} AS next_sink_id",
        "a.atlas_distance_to_sink_km AS distance_to_sink_km",
        f"TIMESTAMP {_literal(timestamp)} AS valid_from",
        "CAST(NULL AS TIMESTAMP) AS valid_to",
        "h.hydrobasins_object_id AS hydrobasins_object_id",
        "a.basinatlas_object_id AS basinatlas_object_id",
        "'pending' AS quality_status",
    )
    return BasinNormalizationPlan(
        hydro_source_filter=f"source_id = {_literal(config.hydro_source_id)}",
        atlas_source_filter=f"source_id = {_literal(config.atlas_source_id)}",
        hydro_source_projection=hydro_source_projection,
        hydro_geometry_projection=hydro_geometry_projection,
        atlas_projection=atlas_projection,
        output_projection=output_projection,
    )


def normalize_basins(
    bronze_frame: Any,
    config: BasinTopologyConfig,
    *,
    basin_version: str,
    valid_from: datetime,
    basin_ids: tuple[str, ...] | None = None,
) -> Any:
    """Return a lazy canonical basin DataFrame without collecting to the driver."""
    plan = basin_normalization_plan(
        config,
        basin_version=basin_version,
        valid_from=valid_from,
    )
    scope_filter = None
    if basin_ids is not None:
        if not basin_ids or any(not value.isdecimal() for value in basin_ids):
            raise ValueError("Silver basin scope requires numeric basin IDs")
        scope_filter = "basin_id IN (" + ",".join(_literal(value) for value in basin_ids) + ")"
    hydro_source = bronze_frame.filter(plan.hydro_source_filter).selectExpr(
        *plan.hydro_source_projection
    )
    if scope_filter is not None:
        hydro_source = hydro_source.filter(scope_filter)
    hydro = hydro_source.selectExpr(*plan.hydro_geometry_projection).alias("h")
    atlas_source = (
        bronze_frame.filter(plan.atlas_source_filter)
        .selectExpr(*plan.atlas_projection)
    )
    if scope_filter is not None:
        atlas_source = atlas_source.filter(scope_filter)
    atlas = atlas_source.alias("a")
    return hydro.join(
        atlas,
        hydro[plan.join_key] == atlas[plan.join_key],
        "left",
    ).selectExpr(*plan.output_projection)
