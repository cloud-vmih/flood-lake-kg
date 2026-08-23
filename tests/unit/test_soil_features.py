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


def _complete_soil_paths(depth_paths: dict[str, Path]) -> dict[tuple[str, str, str], Path]:
    properties = ("clay", "sand", "silt", "bdod", "cfvo", "wv0010", "wv0033", "wv1500")
    depths = ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")
    return {
        (property_name, depth, statistic): depth_paths[depth]
        for property_name in properties
        for depth in depths
        for statistic in ("mean", "uncertainty")
    }


def test_soil_0_30_weighting_uses_5_10_15_cm_layers() -> None:
    """Dropping a shallow layer or using equal weights changes the 0--30 cm mean."""
    values = {"0-5cm": 10.0, "5-15cm": 20.0, "15-30cm": 40.0}

    assert weighted_depth_value(values, 0, 30) == pytest.approx((10 * 5 + 20 * 10 + 40 * 15) / 30)


def test_soil_predictors_apply_divisors_then_depth_weights(tmp_path: Path) -> None:
    """Raw SoilGrids integers must be scaled before 0--30 cm aggregation."""
    depth_paths: dict[str, Path] = {}
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
        depth_paths[depth] = path
    depth_paths["30-60cm"] = depth_paths["0-5cm"]
    depth_paths["60-100cm"] = depth_paths["0-5cm"]
    depth_paths["100-200cm"] = depth_paths["0-5cm"]
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [11]}, geometry=[box(500_000, 0, 500_060, 60)], crs="EPSG:32648"
    )

    result = depth_weighted_soil(
        _complete_soil_paths(depth_paths), basins, bands_cm=((0, 30), (30, 100))
    )

    assert result.loc[0, "clay_mean_0_30cm"] == pytest.approx((10 * 5 + 20 * 10 + 40 * 15) / 30)
    assert result.loc[0, "clay_mean_0_30cm_valid_pixel_count"] == 12
    assert result.loc[0, "clay_mean_0_30cm_coverage_fraction"] == pytest.approx(1.0)


def test_soil_coverage_ignores_depths_outside_requested_band(tmp_path: Path) -> None:
    """A masked 100--200 cm asset must not lower 0--30 cm coverage evidence."""
    shallow = tmp_path / "shallow.tif"
    deep = tmp_path / "deep.tif"
    for path, value, nodata in ((shallow, 100, -32768), (deep, -32768, -32768)):
        with rasterio.open(
            path, "w", driver="GTiff", width=2, height=2, count=1, dtype="int16",
            crs="EPSG:32648", transform=from_origin(500_000, 60, 30, 30), nodata=nodata,
        ) as destination:
            destination.write(np.full((2, 2), value, dtype="int16"), 1)
    depth_paths = {depth: shallow for depth in ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm")}
    depth_paths["100-200cm"] = deep
    basins = gpd.GeoDataFrame({"HYBAS_ID": [12]}, geometry=[box(500_000, 0, 500_060, 60)], crs="EPSG:32648")

    result = depth_weighted_soil(
        _complete_soil_paths(depth_paths), basins, bands_cm=((0, 30), (30, 100))
    )

    assert result.loc[0, "clay_mean_0_30cm_valid_pixel_count"] == 12
    assert result.loc[0, "clay_mean_0_30cm_coverage_fraction"] == pytest.approx(1.0)


def test_soil_rejects_an_incomplete_96_asset_matrix(tmp_path: Path) -> None:
    """Publishing a partial property/depth/statistic matrix would hide missing sources."""
    paths = _complete_soil_paths({depth: tmp_path / "missing.tif" for depth in ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")})
    paths.pop(("clay", "100-200cm", "uncertainty"))
    basins = gpd.GeoDataFrame({"HYBAS_ID": [13]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:32648")

    with pytest.raises(ValueError, match="complete 96-asset"):
        depth_weighted_soil(paths, basins, bands_cm=((0, 30), (30, 100)))


def test_soil_requires_both_configured_depth_bands(tmp_path: Path) -> None:
    """A one-band table would omit the mandated 30--100 cm predictor."""
    paths = _complete_soil_paths({depth: tmp_path / "missing.tif" for depth in ("0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm")})
    basins = gpd.GeoDataFrame({"HYBAS_ID": [14]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:32648")

    with pytest.raises(ValueError, match="both configured depth bands"):
        depth_weighted_soil(paths, basins, bands_cm=((0, 30),))
