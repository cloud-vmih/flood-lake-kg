"""Small-file integration contract for hydro harmonization outputs."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, box
from typer.testing import CliRunner

from flashflood_data.aoi import build_study_areas
from flashflood_data.cli import app
from flashflood_data.config import StudyAreaConfig
from flashflood_data.harmonize.hydro import HydroInputPaths, harmonize_hydro
from flashflood_data.paths import ProjectPaths


def test_harmonize_hydro_writes_deterministic_scope_and_hydro_products(
    project_paths: ProjectPaths,
) -> None:
    fixtures = Path(__file__).parents[1] / "fixtures" / "hydro"
    l10 = gpd.read_file(fixtures / "basins_l10.geojson")
    basin_atlas = l10.assign(dis_m3_pyr=[1.0, 2.0, 3.0])
    rivers = gpd.GeoDataFrame(
        {"HYRIV_ID": [1]}, geometry=[LineString([(102.95, 20.05), (103.25, 20.05)])], crs="EPSG:4326"
    )
    atlas_path = project_paths.dataset / "fixture_basinatlas.geojson"
    rivers_path = project_paths.dataset / "fixture_rivers.geojson"
    basin_atlas.to_file(atlas_path, driver="GeoJSON")
    rivers.to_file(rivers_path, driver="GeoJSON")
    core = box(103.01, 20.01, 103.09, 20.09)
    areas = build_study_areas(core, l10.iloc[:2], box(103.0, 20.0, 103.15, 20.15), StudyAreaConfig())

    outputs = harmonize_hydro(
        project_paths,
        areas,
        inputs=HydroInputPaths(
            l10=fixtures / "basins_l10.geojson",
            l9=fixtures / "basins_l9.geojson",
            l8=fixtures / "basins_l8.geojson",
            basinatlas_l10=atlas_path,
            hydrorivers=rivers_path,
        ),
    )

    assert {path.name for path in outputs} == {
        "core_aoi.geoparquet",
        "hydrological_aoi.geoparquet",
        "environmental_aoi.geoparquet",
        "exposure_aoi.geoparquet",
        "subbasin_l10.geoparquet",
        "subbasin_hierarchy.parquet",
        "basinatlas_l10.geoparquet",
        "river_reach.geoparquet",
    }
    assert set(gpd.read_parquet(project_paths.harmonized / "hydro" / "subbasin_l10.geoparquet").HYBAS_ID) == {
        100,
        101,
    }
    clipped = gpd.read_parquet(project_paths.harmonized / "hydro" / "river_reach.geoparquet")
    assert clipped.geometry.iloc[0].within(areas.hydrological) or clipped.geometry.iloc[0].touches(
        areas.hydrological
    )


def test_harmonize_cli_uses_standard_hydrobasins_not_lake_sample(project_paths: ProjectPaths) -> None:
    """Replacing the standard L10 path must make this command fail before writing outputs."""
    fixtures = Path(__file__).parents[1] / "fixtures" / "hydro"
    config_dir = project_paths.root / "config"
    config_dir.mkdir()
    (config_dir / "study_area.yaml").write_text("{}\n", encoding="utf-8")
    raw = project_paths.dataset / "hybas_as_lev01-12_v1c"
    raw.mkdir(parents=True)
    for level in (8, 9, 10):
        source = fixtures / f"basins_l{level}.geojson"
        target = raw / f"hybas_as_lev{level:02d}_v1c.shp"
        gpd.read_file(source).to_file(target)
    atlas_dir = project_paths.dataset / "BasinATLAS_Data_v10_shp" / "BasinATLAS_v10_shp"
    river_dir = project_paths.dataset / "HydroRIVERS_v10_as_shp" / "HydroRIVERS_v10_as_shp"
    atlas_dir.mkdir(parents=True)
    river_dir.mkdir(parents=True)
    l10 = gpd.read_file(fixtures / "basins_l10.geojson")
    l10.to_file(atlas_dir / "BasinATLAS_v10_lev10.shp")
    gpd.GeoDataFrame(
        {"HYRIV_ID": [1]}, geometry=[LineString([(102.95, 20.05), (103.25, 20.05)])], crs="EPSG:4326"
    ).to_file(river_dir / "HydroRIVERS_v10_as.shp")
    admin_dir = project_paths.harmonized / "admin"
    aoi_dir = project_paths.harmonized / "aoi"
    admin_dir.mkdir(parents=True)
    aoi_dir.mkdir(parents=True)
    gpd.GeoDataFrame(geometry=[box(103.01, 20.01, 103.09, 20.09)], crs="EPSG:4326").to_parquet(
        aoi_dir / "core_aoi.geoparquet", index=False
    )
    gpd.GeoDataFrame(geometry=[box(103.0, 20.0, 103.15, 20.15)], crs="EPSG:4326").to_parquet(
        admin_dir / "vietnam_boundary.geoparquet", index=False
    )

    result = CliRunner().invoke(app, ["harmonize", "--root", str(project_paths.root)])

    assert result.exit_code == 0, result.stdout
    assert '"selected_l10": 2' in result.stdout
