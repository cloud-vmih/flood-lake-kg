"""Display-only MapLibre QA bundle contracts."""

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, box

from flashflood_data.paths import ProjectPaths
from flashflood_data.static.qa.map import publish_qa_map


def _vector(path: Path, name: str, geometry) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"source_id": [name], "name": [name]}, geometry=[geometry], crs="EPSG:4326"
    ).to_parquet(path, index=False)


def test_map_manifest_lists_required_vectors_and_is_display_only(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    layers = {
        "communes": paths.harmonized / "admin" / "admin_commune_2025.geoparquet",
        "subbasins_l10": paths.harmonized / "hydro" / "subbasin_l10.geoparquet",
        "rivers": paths.harmonized / "hydro" / "river_reach.geoparquet",
        "roads": paths.derived / "exposure" / "road_segment.geoparquet",
        "bridges": paths.derived / "exposure" / "bridge.geoparquet",
        "facilities": paths.derived / "exposure" / "facility.geoparquet",
        "settlements": paths.derived / "exposure" / "settlement.geoparquet",
    }
    for name, path in layers.items():
        _vector(
            path,
            name,
            LineString([(0, 0), (0.2, 0.2)])
            if name in {"rivers", "roads"}
            else box(0, 0, 0.2, 0.2),
        )
    before = layers["communes"].read_bytes()

    index = publish_qa_map(paths, paths.qa)

    manifest = json.loads((index.parent / "layer-manifest.json").read_text())
    assert set(manifest["layers"]) >= {
        "communes",
        "subbasins_l10",
        "rivers",
        "roads",
        "bridges",
        "facilities",
        "settlements",
        "dem",
        "worldcover",
        "worldpop",
    }
    assert layers["communes"].read_bytes() == before
    html = index.read_text()
    assert "maplibre-gl@5.6.2" in html
    assert "qa_warning" in html
    assert "raster" in html
    assert "setHTML" not in html
    assert "type: 'circle'" in html
    assert "type: 'fill'" in html
    assert all(not Path(layer["path"]).is_absolute() for layer in manifest["layers"].values())


def test_map_warning_flag_includes_repaired_geometry_and_popup_ids(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    source = paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
    source.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"current_commune_code": ["14001"], "geometry_repaired": [True]},
        geometry=[box(104, 21, 104.1, 21.1)],
        crs="EPSG:4326",
    ).to_parquet(source, index=False)

    index = publish_qa_map(paths, paths.qa)

    feature = json.loads((index.parent / "data" / "communes.geojson").read_text())["features"][0]
    assert feature["properties"]["qa_warning"] is True
    assert "current_commune_code" in index.read_text()
    assert "HYRIV_ID" in index.read_text()


def test_map_joins_task16_boundary_warning_to_river_feature(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    river = paths.harmonized / "hydro" / "river_reach.geoparquet"
    river.parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(
        {"HYRIV_ID": [9]}, geometry=[LineString([(104, 21), (104.1, 21.1)])], crs="EPSG:4326"
    ).to_parquet(river, index=False)
    mapping = paths.derived / "mappings" / "map_subbasin_river.parquet"
    mapping.parent.mkdir(parents=True, exist_ok=True)
    __import__("pandas").DataFrame(
        {"HYRIV_ID": [9], "boundary_case": [True], "quality_flags_json": ["{}"]}
    ).to_parquet(mapping, index=False)

    index = publish_qa_map(paths, paths.qa)

    feature = json.loads((index.parent / "data" / "rivers.geojson").read_text())["features"][0]
    assert feature["properties"]["qa_warning"] is True


def test_map_propagates_mapping_warnings_to_every_display_entity_contract(
    tmp_path: Path,
) -> None:
    """Dropping commune, population, or configurable-ID joins hides real QA evidence."""
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    vector_layers = {
        "communes": (
            paths.harmonized / "admin" / "admin_commune_2025.geoparquet",
            {"current_commune_code": ["C1", "C2"]},
            [box(104, 20, 104.5, 20.5), box(104.5, 20, 105, 20.5)],
        ),
        "subbasins_l10": (
            paths.harmonized / "hydro" / "subbasin_l10.geoparquet",
            {"HYBAS_ID": [1, 2]},
            [box(104, 20, 104.5, 20.5), box(104.5, 20, 105, 20.5)],
        ),
        "rivers": (
            paths.harmonized / "hydro" / "river_reach.geoparquet",
            {"HYRIV_ID": ["R1", "R2"]},
            [LineString([(104, 20.1), (104.4, 20.1)]), LineString([(104.6, 20.1), (105, 20.1)])],
        ),
        "facilities": (
            paths.derived / "exposure" / "facility.geoparquet",
            {"clinic_code": ["F1", "F2"], "osm_id": ["node/1", "node/2"]},
            [Point(104.25, 20.25), Point(104.75, 20.25)],
        ),
    }
    for path, columns, geometry in vector_layers.values():
        path.parent.mkdir(parents=True, exist_ok=True)
        gpd.GeoDataFrame(columns, geometry=geometry, crs="EPSG:4326").to_parquet(path, index=False)
    mapping_dir = paths.derived / "mappings"
    mapping_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "HYBAS_ID": [1],
            "current_commune_code": ["C1"],
            "quality_flags_json": ['{"coverage_sliver":true}'],
        }
    ).to_parquet(mapping_dir / "map_subbasin_commune.parquet", index=False)
    pd.DataFrame(
        {
            "HYBAS_ID": [1],
            "boundary_center_tie_pixel_count": [1],
            "quality_flags_json": ['{"boundary_center_tie_pixel_count":1}'],
        }
    ).to_parquet(mapping_dir / "map_subbasin_population.parquet", index=False)
    pd.DataFrame({"HYBAS_ID": [1], "HYRIV_ID": ["R1"], "boundary_case": [True]}).to_parquet(
        mapping_dir / "map_subbasin_river.parquet", index=False
    )
    pd.DataFrame(
        {
            "HYBAS_ID": [1],
            "clinic_code": ["F1"],
            "boundary_case": [True],
            "quality_flags_json": ["{}"],
        }
    ).to_parquet(mapping_dir / "map_subbasin_facility.parquet", index=False)

    index = publish_qa_map(paths, paths.qa)

    def warnings(layer: str, identifier: str) -> dict[str, bool]:
        features = json.loads((index.parent / "data" / f"{layer}.geojson").read_text())["features"]
        return {
            str(feature["properties"][identifier]): feature["properties"]["qa_warning"]
            for feature in features
        }

    assert warnings("communes", "current_commune_code") == {"C1": True, "C2": False}
    assert warnings("subbasins_l10", "HYBAS_ID") == {"1": True, "2": False}
    assert warnings("rivers", "HYRIV_ID") == {"R1": True, "R2": False}
    assert warnings("facilities", "clinic_code") == {"F1": True, "F2": False}
