"""Lazy Sedona overlay for versioned source-grid to basin area weights."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from flashflood_data.pipelines.silver_weather.config import WeatherAggregationConfig


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_grid_basin_weights(
    spark: Any,
    grid_rows: Any,
    basin_rows: Any,
    config: WeatherAggregationConfig,
) -> Any:
    """Return a lazy Spark frame of positive-area cell/basin intersections.

    Both inputs are expected to be pinned DataFrames from ``reader.py``. Sedona
    performs the spatial join and overlay in the configured equal-area CRS. The
    caller owns filtering to missing versions, DQ, and the Iceberg commit.
    """
    grid_view = f"weather_grid_{uuid4().hex}"
    basin_view = f"weather_basin_{uuid4().hex}"
    crs = _literal(config.equal_area_crs)
    version = _literal(config.geometry_processing_version)
    views: list[str] = []
    try:
        grid_rows.createOrReplaceTempView(grid_view)
        views.append(grid_view)
        basin_rows.createOrReplaceTempView(basin_view)
        views.append(basin_view)

        # ST_Intersects is a spatial join predicate, allowing Sedona's join
        # strategy to prune non-overlapping grid cells before intersection.
        return spark.sql(
            f"""
            WITH grid_geometry AS (
                SELECT source_id, source_grid_version, source_grid_id,
                       NOT ST_IsValid(ST_GeomFromWKB(geometry_wkb)) AS grid_repaired,
                       ST_Transform(ST_MakeValid(ST_GeomFromWKB(geometry_wkb)),
                                    source_grid.crs, {crs}) AS geom
                FROM {grid_view} AS source_grid
            ),
            basin_geometry AS (
                SELECT basin_id, basin_version,
                       NOT ST_IsValid(ST_GeomFromWKB(geometry_wkb)) AS basin_repaired,
                       ST_Transform(ST_MakeValid(ST_GeomFromWKB(geometry_wkb)),
                                    source_basin.crs, {crs}) AS geom
                FROM {basin_view} AS source_basin
            ),
            grid_ready AS (
                SELECT *, ST_Area(geom) AS grid_area_m2
                FROM grid_geometry
                WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
            ),
            basin_ready AS (
                SELECT *, ST_Area(geom) AS basin_area_m2
                FROM basin_geometry
                WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
            ),
            intersections AS (
                SELECT g.source_id, g.source_grid_version, g.source_grid_id,
                       b.basin_id, b.basin_version,
                       g.grid_repaired, b.basin_repaired,
                       g.grid_area_m2, b.basin_area_m2,
                       ST_Area(ST_Intersection(g.geom, b.geom)) AS intersection_area_m2
                FROM grid_ready g
                INNER JOIN basin_ready b ON ST_Intersects(g.geom, b.geom)
                WHERE g.grid_area_m2 > 0 AND b.basin_area_m2 > 0
            )
            SELECT source_id, source_grid_version, source_grid_id,
                   basin_id, basin_version,
                   {version} AS geometry_processing_version,
                   CAST(intersection_area_m2 AS DOUBLE) AS intersection_area_m2,
                   LEAST(1.0, intersection_area_m2 / basin_area_m2) AS weight_by_basin,
                   LEAST(1.0, intersection_area_m2 / grid_area_m2) AS weight_by_grid,
                   CASE WHEN grid_repaired OR basin_repaired
                             OR intersection_area_m2 >
                                LEAST(grid_area_m2, basin_area_m2) * 1.00000001
                        THEN 'warning' ELSE 'passed' END AS quality_status
            FROM intersections
            WHERE intersection_area_m2 > 0
            """
        )
    finally:
        for view in reversed(views):
            spark.catalog.dropTempView(view)
