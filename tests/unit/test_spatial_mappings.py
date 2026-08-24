"""Metric, many-to-many spatial relationship contracts."""

from __future__ import annotations

import json

import geopandas as gpd
import pytest
from geopandas.sindex import SpatialIndex
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


def test_mapping_rejects_near_integral_basin_id_before_cast() -> None:
    """Casting 11.000000001 to basin 11 would silently rewrite the relationship key."""
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [11.000000001]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:32648"
    )
    communes = gpd.GeoDataFrame(
        {"current_commune_code": ["A"]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:32648"
    )

    with pytest.raises(ValueError, match="exact integers"):
        map_subbasin_commune(basins, communes)


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


def test_line_mapping_uses_spatial_index_to_prune_intersection_candidates(monkeypatch) -> None:
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [11, 12]},
        geometry=[box(0, 0, 100, 100), box(1_000, 0, 1_100, 100)],
        crs="EPSG:32648",
    )
    lines = gpd.GeoDataFrame(
        {"segment_id": ["near", "far"]},
        geometry=[LineString([(-20, 50), (120, 50)]), LineString([(10_000, 0), (10_000, 100)])],
        crs="EPSG:32648",
    )
    original_query = SpatialIndex.query
    query_count = 0

    def counted_query(self, *args, **kwargs):
        nonlocal query_count
        query_count += 1
        return original_query(self, *args, **kwargs)

    monkeypatch.setattr(SpatialIndex, "query", counted_query)

    result = map_subbasin_lines(basins, lines, "segment_id")

    assert result.segment_id.tolist() == ["near"]
    assert query_count == len(basins)


@pytest.mark.parametrize("invalid_id", [float("inf"), " "])
def test_line_mapping_rejects_invalid_entity_identity(invalid_id: object) -> None:
    """Null-like or nonfinite source identities cannot become relationship foreign keys."""
    basins = gpd.GeoDataFrame({"HYBAS_ID": [11]}, geometry=[box(0, 0, 100, 100)], crs="EPSG:32648")
    rivers = gpd.GeoDataFrame(
        {"HYRIV_ID": [invalid_id]},
        geometry=[LineString([(-20, 50), (120, 50)])],
        crs="EPSG:32648",
    )

    with pytest.raises(ValueError, match="invalid HYRIV_ID"):
        map_subbasin_lines(basins, rivers, "HYRIV_ID")


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
        {"HYRIV_ID": [70], "HYBAS_ID": [999], "quality_flags_json": ['{"fake":true}']},
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


def test_polygon_facility_is_mapped_by_representative_point_across_basin_boundary() -> None:
    basins = gpd.GeoDataFrame(
        {"HYBAS_ID": [11, 12]},
        geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)],
        crs="EPSG:32648",
    )
    facilities = gpd.GeoDataFrame(
        {"facility_id": ["hospital-1"], "name": ["Hospital"]},
        geometry=[box(9, 4, 11, 6)],
        crs="EPSG:32648",
    )

    result = map_subbasin_points(basins, facilities, "facility_id")

    assert result.HYBAS_ID.tolist() == [11, 12]
    assert result.relationship_type.tolist() == ["nearest_boundary_tie", "nearest_boundary_tie"]
    assert result.boundary_case.tolist() == [True, True]
