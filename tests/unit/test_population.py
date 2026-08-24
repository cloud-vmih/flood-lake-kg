"""Core-AOI-only WorldPop aggregation contracts."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
import shapely
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.derive.population import _assign_points_to_zones, aggregate_population_by_basin


def _worldpop(path: Path, values: list[list[float]], transform, *, crs: str = "EPSG:32648") -> Path:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=len(values[0]),
        height=len(values),
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=-9999.0,
    ) as destination:
        destination.write(np.asarray(values, dtype="float32"), 1)
    return path


def test_pixel_zone_assignment_is_vectorized_and_resolves_boundary_ties() -> None:
    zones = {20: box(0, 0, 10, 10), 10: box(10, 0, 20, 10)}
    pixel_centers = shapely.points([5, 10, 25], [5, 5, 5])

    winners, match_counts = _assign_points_to_zones(pixel_centers, zones)

    assert winners.tolist() == [20, 10, -1]
    assert match_counts.tolist() == [1, 2, 0]


def test_population_is_clipped_to_core_not_whole_upstream_basin(tmp_path: Path) -> None:
    """Summing the full upstream basin would incorrectly include the third 30-person cell."""
    worldpop = _worldpop(tmp_path / "worldpop.tif", [[10, 20, 30]], from_origin(0, 10, 10, 10))
    basins = gpd.GeoDataFrame({"HYBAS_ID": [7]}, geometry=[box(0, 0, 30, 10)], crs="EPSG:32648")

    result = aggregate_population_by_basin(worldpop, basins, box(0, 0, 20, 10))

    assert result.loc[0, "population_scope"] == "core_aoi_only"
    assert result.loc[0, "population_sum"] == pytest.approx(30.0)
    assert result.loc[0, "population_mean"] == pytest.approx(15.0)
    assert result.loc[0, "contributing_pixel_count"] == 2
    assert result.loc[0, "aoi_pixel_count"] == 2
    assert result.loc[0, "coverage_ratio"] == pytest.approx(1.0)


def test_constrained_worldpop_nodata_is_zero_population_with_evidence(tmp_path: Path) -> None:
    worldpop = _worldpop(
        tmp_path / "worldpop-constrained.tif",
        [[10, -9999]],
        from_origin(0, 10, 10, 10),
    )
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [1]}, geometry=[box(0, 0, 20, 10)], crs="EPSG:32648"
    )

    result = aggregate_population_by_basin(worldpop, basins, box(0, 0, 20, 10))

    assert result.loc[0, "population_sum"] == pytest.approx(10.0)
    assert result.loc[0, "population_mean"] == pytest.approx(5.0)
    assert result.loc[0, "contributing_pixel_count"] == 1
    assert result.loc[0, "nodata_pixel_count"] == 1
    assert result.loc[0, "aoi_pixel_count"] == 2
    assert result.loc[0, "source_resolution_x"] == pytest.approx(10.0)
    assert result.loc[0, "source_resolution_y"] == pytest.approx(10.0)
    assert result.loc[0, "source_resolution_unit"] == "metre"


def test_boundary_center_pixel_is_counted_once_by_lower_hybas_id(tmp_path: Path) -> None:
    """Duplicating a pixel whose center is on a shared boundary would overstate population."""
    worldpop = _worldpop(tmp_path / "worldpop.tif", [[10, 20, 30]], from_origin(-5, 10, 10, 10))
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [20, 10]}, geometry=[box(-5, 0, 10, 10), box(10, 0, 15, 10)], crs="EPSG:32648"
    )

    result = aggregate_population_by_basin(worldpop, basins, box(-5, 0, 15, 10))

    by_id = result.set_index("HYBAS_ID")
    assert by_id.loc[10, "population_sum"] == pytest.approx(20.0)
    assert by_id.loc[20, "population_sum"] == pytest.approx(10.0)
    assert by_id.loc[10, "boundary_center_tie_pixel_count"] == 1
    assert by_id.loc[20, "boundary_center_tie_pixel_count"] == 0
    assert result.population_sum.sum() == pytest.approx(30.0)


def test_population_reports_native_geographic_x_y_resolution_without_false_metres(tmp_path: Path) -> None:
    """Collapsing non-square degrees into a scalar metre value misstates the source grid."""
    worldpop = _worldpop(
        tmp_path / "worldpop-geographic.tif",
        [[10, 20]],
        from_origin(104, 21, 0.25, 0.5),
        crs="EPSG:4326",
    )
    basins = gpd.GeoDataFrame({"HYBAS_ID": [7]}, geometry=[box(104, 20, 104.5, 21)], crs="EPSG:4326")

    result = aggregate_population_by_basin(worldpop, basins, box(104, 20, 104.5, 21))

    assert result.loc[0, "source_resolution_x"] == pytest.approx(0.25)
    assert result.loc[0, "source_resolution_y"] == pytest.approx(0.5)
    assert result.loc[0, "source_resolution_unit"] == "degree"
    assert result.loc[0, "source_crs"] == "EPSG:4326"
    assert "source_resolution_m" not in result
