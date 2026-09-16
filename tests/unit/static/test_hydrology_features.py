"""Numerical contracts for basin-scale hydrography predictors."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import LineString, box

from flashflood_data.static.features.hydrology import derive_hydrology_features


@pytest.fixture
def basin() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"HYBAS_ID": [31]}, geometry=[box(500_000, 0, 501_000, 800)], crs="EPSG:32648"
    )


@pytest.fixture
def rivers() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        geometry=[
            LineString([(500_000, 200), (501_000, 200)]),
            LineString([(500_000, 600), (501_000, 600)]),
        ],
        crs="EPSG:32648",
    )


def test_drainage_density_is_length_over_area(
    rivers: gpd.GeoDataFrame, basin: gpd.GeoDataFrame
) -> None:
    """Changing river length or basin area changes drainage density in km/km²."""
    result = derive_hydrology_features(rivers, basin, dem_path=None, basinatlas=None)

    assert result.loc[0, "HYBAS_ID"] == 31
    assert result.loc[0, "drainage_density_km_per_km2"] == pytest.approx(2.5)
    assert result.loc[0, "river_length_km"] == pytest.approx(2.0)
    assert result.loc[0, "river_reach_count"] == 2


def test_hydrology_requires_configured_basinatlas_fields(
    rivers: gpd.GeoDataFrame, basin: gpd.GeoDataFrame
) -> None:
    """A silently partial BasinATLAS join would make its provenance untrustworthy."""
    incomplete = basin[["HYBAS_ID", "geometry"]].copy()

    with pytest.raises(ValueError, match="BasinATLAS is missing required fields"):
        derive_hydrology_features(rivers, basin, dem_path=None, basinatlas=incomplete)


def test_hydrology_rejects_nonfinite_basinatlas_values(
    rivers: gpd.GeoDataFrame, basin: gpd.GeoDataFrame
) -> None:
    """Infinite baseline fields must be rejected before any Parquet writer runs."""
    from flashflood_data.static.features.config import load_feature_config

    atlas = basin.copy()
    for field in load_feature_config().basinatlas_fields:
        atlas[field] = 1.0
    atlas.loc[0, "pre_mm_syr"] = float("inf")

    with pytest.raises(ValueError, match="nonfinite BasinATLAS"):
        derive_hydrology_features(rivers, basin, dem_path=None, basinatlas=atlas)
