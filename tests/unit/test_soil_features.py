"""Numerical contracts for SoilGrids depth-weighted predictors."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.derive.soil import depth_weighted_soil, weighted_depth_value


def test_soil_0_30_weighting_uses_5_10_15_cm_layers() -> None:
    """Dropping a shallow layer or using equal weights changes the 0--30 cm mean."""
    values = {"0-5cm": 10.0, "5-15cm": 20.0, "15-30cm": 40.0}

    assert weighted_depth_value(values, 0, 30) == pytest.approx((10 * 5 + 20 * 10 + 40 * 15) / 30)


def test_soil_predictors_apply_divisors_then_depth_weights(tmp_path: Path) -> None:
    """Raw SoilGrids integers must be scaled before 0--30 cm aggregation."""
    paths: dict[tuple[str, str, str], Path] = {}
    raw_values = {"0-5cm": 100, "5-15cm": 200, "15-30cm": 400}
    for depth, value in raw_values.items():
        path = tmp_path / f"clay_{depth}_mean.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=2,
            height=2,
            count=1,
            dtype="int16",
            crs="EPSG:32648",
            transform=from_origin(500_000, 60, 30, 30),
            nodata=-32768,
        ) as destination:
            destination.write(np.full((2, 2), value, dtype="int16"), 1)
        paths[("clay", depth, "mean")] = path
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [11]}, geometry=[box(500_000, 0, 500_060, 60)], crs="EPSG:32648"
    )

    result = depth_weighted_soil(paths, basins, bands_cm=((0, 30),))

    assert result.loc[0, "clay_mean_0_30cm"] == pytest.approx((10 * 5 + 20 * 10 + 40 * 15) / 30)
    assert result.loc[0, "clay_mean_0_30cm_valid_pixel_count"] == 12
    assert result.loc[0, "clay_mean_0_30cm_coverage_fraction"] == pytest.approx(1.0)
