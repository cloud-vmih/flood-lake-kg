"""Numerical contracts for metric terrain predictors."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.derive.terrain import derive_terrain_features


@pytest.fixture
def synthetic_dem(tmp_path: Path) -> Path:
    """A 30 m metric DEM with a hand-derived 90 m relief."""
    path = tmp_path / "dem.tif"
    values = np.array(
        [[0.0, 30.0, 60.0, 90.0]] * 4,
        dtype="float32",
    )
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="float32",
        crs="EPSG:32648",
        transform=from_origin(500_000, 1_000, 30, 30),
        nodata=-9999,
    ) as destination:
        destination.write(values, 1)
    return path


@pytest.fixture
def basin() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"HYBAS_ID": [101]},
        geometry=[box(500_000, 880, 500_120, 1_000)],
        crs="EPSG:32648",
    )


def test_relief_and_slope_are_metric(synthetic_dem: Path, basin: gpd.GeoDataFrame) -> None:
    """Changing a DEM's elevation range or metric gradient changes its predictors."""
    result = derive_terrain_features(synthetic_dem, basin, processing_crs="EPSG:32648")

    assert result.loc[0, "HYBAS_ID"] == 101
    assert result.loc[0, "elevation_relief_m"] == pytest.approx(90.0)
    assert result.loc[0, "slope_deg_mean"] == pytest.approx(45.0)
    assert result.loc[0, "dem_valid_pixel_count"] == 16
    assert result.loc[0, "dem_coverage_fraction"] == pytest.approx(1.0)


def test_terrain_rejects_duplicate_or_nonfinite_basin_ids(
    synthetic_dem: Path, basin: gpd.GeoDataFrame
) -> None:
    """Duplicate identifiers would make a feature table ambiguous."""
    duplicate = basin.loc[[0, 0]].copy()

    with pytest.raises(ValueError, match="duplicate HYBAS_ID"):
        derive_terrain_features(synthetic_dem, duplicate, processing_crs="EPSG:32648")


def test_metric_dem_working_raster_is_retained_under_dataset_derived(
    synthetic_dem: Path, basin: gpd.GeoDataFrame, tmp_path: Path
) -> None:
    """Deleting the metric working grid would make terrain outputs unreproducible."""
    working_input = tmp_path / "dataset" / "harmonized" / "dem.tif"
    working_input.parent.mkdir(parents=True)
    working_input.write_bytes(synthetic_dem.read_bytes())

    derive_terrain_features(working_input, basin, processing_crs="EPSG:32648")

    retained = list((tmp_path / "dataset" / "derived" / "terrain").glob("*.tif"))
    assert len(retained) == 1
    with rasterio.open(retained[0]) as dataset:
        assert dataset.crs.to_string() == "EPSG:32648"
        assert dataset.res == pytest.approx((30.0, 30.0))
