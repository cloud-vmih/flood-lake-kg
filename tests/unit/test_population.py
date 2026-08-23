"""Core-AOI-only WorldPop aggregation contracts."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.derive.population import aggregate_population_by_basin


def _worldpop(path: Path, values: list[list[float]], transform) -> Path:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=len(values[0]),
        height=len(values),
        count=1,
        dtype="float32",
        crs="EPSG:32648",
        transform=transform,
        nodata=-9999.0,
    ) as destination:
        destination.write(np.asarray(values, dtype="float32"), 1)
    return path


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
    assert result.loc[0, "source_resolution_m"] == pytest.approx(10.0)


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
