"""Metric spatial relationships between selected basins and entities."""

from __future__ import annotations

import json

import geopandas as gpd
import numpy as np
import pandas as pd

from flashflood_data.static.features.spatial import checked_basins

PROCESSING_CRS = "EPSG:32648"
_LINE_RESERVED_OUTPUT_COLUMNS = frozenset(
    {
        "HYBAS_ID",
        "intersected_length_km",
        "boundary_case",
        "quality_flags_json",
        "processing_crs",
        "source_asset_ids_json",
        "relationship_geometry_wkt",
    }
)


def _metric_layers(
    basins: gpd.GeoDataFrame, entities: gpd.GeoDataFrame, entity_id: str
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Validate one relationship input and put both layers in the metric overlay CRS."""
    if entities.crs is None:
        raise ValueError("entities must have a CRS")
    if entity_id not in entities.columns:
        raise ValueError(f"entities are missing required {entity_id} column")
    invalid = entities[entity_id].map(
        lambda value: (
            value is None
            or isinstance(value, (bool, np.bool_))
            or bool(pd.isna(value))
            or (isinstance(value, (float, np.floating)) and not np.isfinite(value))
            or (isinstance(value, str) and not value.strip())
        )
    )
    if invalid.any():
        raise ValueError(f"entities have invalid {entity_id} values")
    if entities[entity_id].duplicated().any():
        raise ValueError(f"entities have missing or duplicate {entity_id} values")
    return checked_basins(basins).to_crs(PROCESSING_CRS), entities.to_crs(PROCESSING_CRS)


def _provenance() -> dict[str, object]:
    return {"processing_crs": PROCESSING_CRS, "source_asset_ids_json": "[]"}


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def map_subbasin_commune(basins: gpd.GeoDataFrame, communes: gpd.GeoDataFrame) -> pd.DataFrame:
    """Return unsimplified metric basin--current-commune area relationships."""
    metric_basins, metric_communes = _metric_layers(basins, communes, "current_commune_code")
    rows: list[dict[str, object]] = []
    for basin in metric_basins.itertuples(index=False):
        basin_area = float(basin.geometry.area)
        for commune in metric_communes.itertuples(index=False):
            intersection = basin.geometry.intersection(commune.geometry)
            area = float(intersection.area)
            if intersection.is_empty or area <= 0:
                continue
            commune_area = float(commune.geometry.area)
            rows.append(
                {
                    "HYBAS_ID": int(basin.HYBAS_ID),
                    "current_commune_code": str(commune.current_commune_code),
                    "intersection_area_km2": area / 1_000_000,
                    "basin_fraction": area / basin_area if basin_area else 0.0,
                    "commune_fraction": area / commune_area if commune_area else 0.0,
                    "quality_flags_json": "{}",
                    **_provenance(),
                }
            )
    columns = [
        "HYBAS_ID",
        "current_commune_code",
        "intersection_area_km2",
        "basin_fraction",
        "commune_fraction",
        "quality_flags_json",
        "processing_crs",
        "source_asset_ids_json",
    ]
    if not rows:
        return _empty(columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .sort_values(["HYBAS_ID", "current_commune_code"], kind="stable")
        .reset_index(drop=True)
    )


def map_subbasin_lines(
    basins: gpd.GeoDataFrame,
    lines: gpd.GeoDataFrame,
    entity_id: str,
    *,
    include_relationship_geometry: bool = False,
) -> pd.DataFrame:
    """Return metric line intersections, retaining source identifiers and boundary evidence."""
    metric_basins, metric_lines = _metric_layers(basins, lines, entity_id)
    extra_columns = [
        column for column in lines.columns if column != "geometry" and column != entity_id
    ]
    collisions = sorted(set(extra_columns) & _LINE_RESERVED_OUTPUT_COLUMNS)
    if collisions:
        raise ValueError(
            "source line columns collide with reserved output names: " + ", ".join(collisions)
        )
    rows: list[dict[str, object]] = []
    spatial_index = metric_lines.sindex
    for basin in metric_basins.itertuples(index=False):
        candidate_indexes = spatial_index.query(basin.geometry, predicate="intersects")
        for _, line in metric_lines.iloc[candidate_indexes].iterrows():
            intersection = basin.geometry.intersection(line.geometry)
            length = float(intersection.length)
            if intersection.is_empty or length <= 0:
                continue
            row = {
                "HYBAS_ID": int(basin.HYBAS_ID),
                entity_id: line[entity_id],
                "intersected_length_km": length / 1_000,
                "boundary_case": bool(line.geometry.touches(basin.geometry)),
                "quality_flags_json": "{}",
                **_provenance(),
            }
            if include_relationship_geometry:
                row["relationship_geometry_wkt"] = intersection.wkt
            row.update({column: line[column] for column in extra_columns})
            rows.append(row)
    columns = [
        "HYBAS_ID",
        entity_id,
        *extra_columns,
        "intersected_length_km",
        "boundary_case",
        "quality_flags_json",
        "processing_crs",
        "source_asset_ids_json",
    ]
    if include_relationship_geometry:
        columns.append("relationship_geometry_wkt")
    if not rows:
        return _empty(columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .sort_values(["HYBAS_ID", entity_id], kind="stable")
        .reset_index(drop=True)
    )


def map_subbasin_points(
    basins: gpd.GeoDataFrame, points: gpd.GeoDataFrame, entity_id: str
) -> pd.DataFrame:
    """Map point-like entities by an interior point, preserving exact boundary ties."""
    metric_basins, metric_points = _metric_layers(basins, points, entity_id)
    rows: list[dict[str, object]] = []
    tag_columns = [column for column in points.columns if column not in {"geometry", entity_id}]
    for _, point in metric_points.iterrows():
        location = (
            point.geometry
            if point.geometry.geom_type == "Point"
            else point.geometry.representative_point()
        )
        matches = metric_basins.loc[metric_basins.geometry.covers(location)]
        tie = len(matches) > 1 and any(
            geometry.touches(location) for geometry in matches.geometry
        )
        for basin in matches.itertuples(index=False):
            touches = bool(basin.geometry.touches(location))
            relationship = "within" if location.within(basin.geometry) else "touches"
            if tie and touches:
                relationship = "nearest_boundary_tie"
            tags = {column: point[column] for column in tag_columns if pd.notna(point[column])}
            rows.append(
                {
                    "HYBAS_ID": int(basin.HYBAS_ID),
                    entity_id: point[entity_id],
                    "relationship_type": relationship,
                    "tags_json": json.dumps(tags, ensure_ascii=False, sort_keys=True, default=str),
                    "boundary_case": touches,
                    "quality_flags_json": json.dumps(
                        {"boundary_tie": True} if tie and touches else {}, sort_keys=True
                    ),
                    **_provenance(),
                }
            )
    columns = [
        "HYBAS_ID",
        entity_id,
        "relationship_type",
        "tags_json",
        "boundary_case",
        "quality_flags_json",
        "processing_crs",
        "source_asset_ids_json",
    ]
    if not rows:
        return _empty(columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .sort_values(["HYBAS_ID", entity_id], kind="stable")
        .reset_index(drop=True)
    )
