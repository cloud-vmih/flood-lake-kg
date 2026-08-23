"""Display-only MapLibre QA bundle contracts."""

import json
from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, box

from flashflood_data.paths import ProjectPaths
from flashflood_data.qa.map import publish_qa_map


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
