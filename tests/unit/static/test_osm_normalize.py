"""Fixture-driven tests for exact Exposure-AOI OSM normalisation."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
from shapely.geometry import box

from flashflood_data.static.sources.osm import _with_id, extract_osm_layers, normalize_roads

FIXTURES = Path(__file__).parents[2] / "fixtures" / "osm"


def test_osm_segments_have_stable_source_identity() -> None:
    roads = normalize_roads(gpd.read_file(FIXTURES / "lines.geojson"))

    assert roads.segment_id.tolist() == ["way/42:000", "way/42:001"]
    assert roads.osm_id.tolist() == ["way/42", "way/42"]


def test_closed_way_polygon_uses_osm_way_id_when_relation_id_is_null() -> None:
    layer = gpd.GeoDataFrame(
        {"osm_id": [None], "osm_way_id": [42]},
        geometry=[box(103, 20, 104, 21)],
        crs="EPSG:4326",
    )

    normalized = _with_id(layer, "relation")

    assert normalized.osm_id.tolist() == ["way/42"]


def test_fixture_extraction_clips_and_preserves_unapproved_source_tags(
    monkeypatch, tmp_path: Path
) -> None:
    layers = {
        "points": gpd.read_file(FIXTURES / "points.geojson"),
        "lines": gpd.read_file(FIXTURES / "lines.geojson"),
        "multilinestrings": gpd.GeoDataFrame(geometry=[], crs="EPSG:4326"),
        "multipolygons": gpd.read_file(FIXTURES / "multipolygons.geojson"),
    }

    def read_dataframe(_path: Path, *, layer: str, **_kwargs: object) -> gpd.GeoDataFrame:
        return layers[layer]

    monkeypatch.setattr("flashflood_data.static.sources.osm.pyogrio.read_dataframe", read_dataframe)
    outputs = extract_osm_layers(
        tmp_path / "fixture.osm.pbf",
        box(103.0, 20.0, 104.0, 21.0),
        Path("config/osmconf.ini"),
        output_dir=tmp_path / "exposure",
    )

    roads = gpd.read_parquet(outputs.roads)
    facilities = gpd.read_parquet(outputs.facilities)
    settlements = gpd.read_parquet(outputs.settlements)
    water = gpd.read_parquet(outputs.water_context)

    assert roads.segment_id.tolist() == ["way/42:000", "way/42:001"]
    assert facilities.osm_id.tolist() == ["node/7"]
    assert json.loads(facilities.tags_json.iloc[0])["operator"] == "fixture clinic"
    assert settlements.osm_id.tolist() == ["node/8"]
    assert water.osm_id.tolist() == ["relation/9"]
    assert all(geometry.within(box(103.0, 20.0, 104.0, 21.0)) for geometry in roads.geometry)
