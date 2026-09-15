"""Shared validation, repair, and atomic persistence for vector layers."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd
from shapely import make_valid

from flashflood_data.catalog.models import ValidationResult
from flashflood_data.io_atomic import atomic_target


def repair_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Return a repaired copy of *gdf* with explicit geometry-repair provenance."""
    repaired = gdf.copy()
    was_valid = repaired.geometry.notna() & repaired.geometry.is_valid
    repaired["geometry_was_valid"] = was_valid
    repaired["geometry_repaired"] = ~was_valid & repaired.geometry.notna()
    repaired.loc[repaired["geometry_repaired"], "geometry"] = repaired.loc[
        repaired["geometry_repaired"], "geometry"
    ].map(make_valid)
    return repaired


def validate_vector(
    gdf: gpd.GeoDataFrame,
    required_columns: Sequence[str],
    expected_crs: str,
) -> ValidationResult:
    """Check schema, CRS, and geometry integrity without changing the source layer."""
    checks = {
        "non_empty": not gdf.empty,
        "required_columns": set(required_columns).issubset(gdf.columns),
        "expected_crs": gdf.crs is not None and gdf.crs.to_string() == expected_crs,
        "geometry_present": bool(gdf.geometry.notna().all()),
        "geometry_valid": bool(gdf.geometry.notna().all() and gdf.geometry.is_valid.all()),
    }
    return ValidationResult(
        passed=all(checks.values()),
        checks=checks,
        metrics={"feature_count": len(gdf)},
        messages=()
        if all(checks.values())
        else ("vector layer does not meet schema, CRS, or geometry requirements",),
    )


def write_geoparquet(
    gdf: gpd.GeoDataFrame,
    path: Path,
    storage_crs: str = "EPSG:4326",
) -> Path:
    """Atomically write a reprojected GeoParquet copy, leaving *gdf* untouched."""
    if gdf.crs is None:
        raise ValueError("cannot write vector data without a CRS")
    stored = gdf.to_crs(storage_crs)
    with atomic_target(path) as partial:
        stored.to_parquet(partial, index=False)
    return path
