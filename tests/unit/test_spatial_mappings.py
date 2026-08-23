"""Metric, many-to-many spatial relationship contracts."""

from __future__ import annotations

import json

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Point, box

from flashflood_data.derive.mappings import (
    map_subbasin_commune,
    map_subbasin_lines,
    map_subbasin_points,
)


def test_commune_mapping_carries_both_hand_derived_area_fractions() -> None:
    """Using either parent area as both denominators would corrupt an areal crosswalk."""
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [11, 12]},
        geometry=[box(0, 0, 50, 100), box(50, 0, 100, 100)],
        crs="EPSG:32648",
    )
    communes = gpd.GeoDataFrame(
        {"current_commune_code": ["A"]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:32648"
    )

    result = map_subbasin_commune(basins, communes)

    assert result[["HYBAS_ID", "current_commune_code"]].values.tolist() == [[11, "A"], [12, "A"]]
    assert result.intersection_area_km2.tolist() == pytest.approx([0.005, 0.005])
    assert result.basin_fraction.tolist() == pytest.approx([1.0, 1.0])
    assert result.commune_fraction.tolist() == pytest.approx([0.5, 0.5])
    assert result.quality_flags_json.map(json.loads).tolist() == [{}, {}]
    assert set(result) >= {"source_asset_ids_json", "processing_crs"}


def test_line_mapping_reports_metric_intersection_length() -> None:
    """Measuring in geographic degrees instead of EPSG:32648 would return the wrong length."""
    basins = gpd.GeoDataFrame({"HYBAS_ID": [11]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:32648")
    rivers = gpd.GeoDataFrame(
        {"HYRIV_ID": [70]}, geometry=[LineString([(-20, 50), (120, 50)])], crs="EPSG:32648"
    )

    result = map_subbasin_lines(basins, rivers, "HYRIV_ID")

    assert result.loc[0, "HYBAS_ID"] == 11
    assert result.loc[0, "HYRIV_ID"] == 70
    assert result.loc[0, "intersected_length_km"] == pytest.approx(0.1)
    assert result.loc[0, "boundary_case"] == False


def test_bridge_mapping_can_preserve_the_unsimplified_relationship_geometry() -> None:
    """Dropping a bridge's clipped geometry would remove the explicit geometry evidence product."""
    basins = gpd.GeoDataFrame({"HYBAS_ID": [11]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:32648")
    bridges = gpd.GeoDataFrame(
        {"bridge_id": ["b-1"], "osm_id": [123]},
        geometry=[LineString([(-20, 50), (120, 50)])],
        crs="EPSG:32648",
    )

    result = map_subbasin_lines(basins, bridges, "bridge_id", include_relationship_geometry=True)

    assert result.loc[0, "osm_id"] == 123
    assert result.loc[0, "relationship_geometry_wkt"] == "LINESTRING (0 50, 100 50)"


def test_line_mapping_rejects_source_columns_that_would_overwrite_evidence() -> None:
    """Allowing source HYBAS_ID or flags to overwrite overlay evidence corrupts the relationship."""
    basins = gpd.GeoDataFrame({"HYBAS_ID": [11]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:32648")
    rivers = gpd.GeoDataFrame(
        {"HYRIV_ID": [70], "HYBAS_ID": [999], "quality_flags_json": ["{\"fake\":true}"]},
        geometry=[LineString([(-20, 50), (120, 50)])],
        crs="EPSG:32648",
    )

    with pytest.raises(ValueError, match="reserved output names"):
        map_subbasin_lines(basins, rivers, "HYRIV_ID")


def test_boundary_point_emits_every_tied_basin_not_an_arbitrary_winner() -> None:
    """Replacing tied relations with one basin assignment would hide boundary ambiguity."""
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [11, 12]}, geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)], crs="EPSG:32648"
    )
    facilities = gpd.GeoDataFrame(
        {"facility_id": ["clinic-1"], "name": ["Clinic"]}, geometry=[Point(10, 5)], crs="EPSG:32648"
    )

    result = map_subbasin_points(basins, facilities, "facility_id")

    assert result.HYBAS_ID.tolist() == [11, 12]
    assert result.relationship_type.tolist() == ["nearest_boundary_tie", "nearest_boundary_tie"]
    assert result.boundary_case.tolist() == [True, True]
    assert result.tags_json.map(json.loads).tolist() == [{"name": "Clinic"}, {"name": "Clinic"}]
