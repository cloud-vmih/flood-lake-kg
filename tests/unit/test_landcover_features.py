"""Numerical contracts for native-grid WorldCover fractions."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from flashflood_data.static.features.landcover import derive_landcover_fractions


@pytest.fixture
def class_raster(tmp_path: Path) -> Path:
    path = tmp_path / "worldcover.tif"
    values = np.array([[10, 10], [20, 0]], dtype="uint8")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="uint8",
        crs="EPSG:32648",
        transform=from_origin(500_000, 60, 30, 30),
        nodata=0,
    ) as destination:
        destination.write(values, 1)
    return path


@pytest.fixture
def basin() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"HYBAS_ID": [21]}, geometry=[box(500_000, 0, 500_060, 60)], crs="EPSG:32648"
    )


def test_landcover_fractions_sum_to_one(class_raster: Path, basin: gpd.GeoDataFrame) -> None:
    """Valid WorldCover classes are normalized independently from nodata."""
    result = derive_landcover_fractions(class_raster, basin)
    fraction_columns = [column for column in result if column.startswith("landcover_fraction_")]

    assert result[fraction_columns].sum(axis=1).iloc[0] == pytest.approx(1.0)
    assert result.loc[0, "landcover_fraction_tree_cover"] == pytest.approx(2 / 3)
    assert result.loc[0, "landcover_fraction_shrubland"] == pytest.approx(1 / 3)
    assert result.loc[0, "landcover_nodata_pixel_count"] == 1
    assert result.loc[0, "landcover_valid_pixel_count"] == 3
