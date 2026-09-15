"""Small-file integration contract for hydro harmonization outputs."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, box
from typer.testing import CliRunner

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.cli import app
from flashflood_data.config import StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.paths import ProjectPaths
from flashflood_data.pipeline import Stage, StaticPipeline
from flashflood_data.static.harmonize.aoi import build_study_areas
from flashflood_data.static.harmonize.hydro import HydroInputPaths, harmonize_hydro
from flashflood_data.static.sources.registry import load_source_specs


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


def test_three_hydro_source_specs_build_one_checksum_stable_composite(
    project_paths: ProjectPaths,
) -> None:
    """The three real source passes must share one stable composite owner."""
    fixtures = Path(__file__).parents[1] / "fixtures" / "hydro"
    raw = project_paths.dataset / "hybas_as_lev01-12_v1c"
    raw.mkdir(parents=True)
    hydro_paths: list[Path] = []
    for level in (8, 9, 10):
        target = raw / f"hybas_as_lev{level:02d}_v1c.shp"
        gpd.read_file(fixtures / f"basins_l{level}.geojson").to_file(target)
        hydro_paths.append(target)
    atlas_dir = project_paths.dataset / "BasinATLAS_Data_v10_shp" / "BasinATLAS_v10_shp"
    river_dir = project_paths.dataset / "HydroRIVERS_v10_as_shp" / "HydroRIVERS_v10_as_shp"
    atlas_dir.mkdir(parents=True)
    river_dir.mkdir(parents=True)
    l10 = gpd.read_file(fixtures / "basins_l10.geojson")
    atlas = atlas_dir / "BasinATLAS_v10_lev10.shp"
    rivers = river_dir / "HydroRIVERS_v10_as.shp"
    l10.to_file(atlas)
    gpd.GeoDataFrame(
        {"HYRIV_ID": [1]},
        geometry=[LineString([(102.95, 20.05), (103.25, 20.05)])],
        crs="EPSG:4326",
    ).to_file(rivers)
    (project_paths.harmonized / "aoi").mkdir(parents=True)
    (project_paths.harmonized / "admin").mkdir(parents=True)
    gpd.GeoDataFrame(
        geometry=[box(103.01, 20.01, 103.09, 20.09)], crs="EPSG:4326"
    ).to_parquet(project_paths.harmonized / "aoi" / "core_aoi.geoparquet", index=False)
    gpd.GeoDataFrame(
        geometry=[box(103.0, 20.0, 103.15, 20.15)], crs="EPSG:4326"
    ).to_parquet(
        project_paths.harmonized / "admin" / "vietnam_boundary.geoparquet", index=False
    )
    catalog = AssetCatalog(project_paths)
    raw_families = {
        "hydrobasins_v1c": ("1c", hydro_paths),
        "basinatlas_v10": ("10", [atlas]),
        "hydrorivers_v10": ("10", [rivers]),
    }
    for source_id, (version, paths) in raw_families.items():
        for index, path in enumerate(paths):
            catalog.upsert(
                AssetRecord(
                    asset_id=f"raw-{source_id}-{index}",
                    source_id=source_id,
                    source_version=version,
                    kind=AssetKind.RAW,
                    source_uri=f"https://fixture.invalid/{source_id}/{index}",
                    storage_path=str(path),
                    media_type="application/x-esri-shapefile",
                    size_bytes=path.stat().st_size,
                    checksum=sha256_file(path),
                    retrieved_at=datetime(2026, 8, 21, tzinfo=UTC),
                    license_id="fixture-license",
                    pipeline_run_id="hydro-fixture",
                    status=AssetStatus.VALIDATED,
                )
            )
    configured = load_source_specs(Path(__file__).parents[2] / "config" / "sources")
    specs = {source_id: configured[source_id] for source_id in raw_families}
    pipeline = StaticPipeline(project_paths, source_specs=specs, stage_handlers={})

    first = pipeline.run([Stage.HARMONIZE])
    before = {
        path.relative_to(project_paths.dataset).as_posix(): sha256_file(path)
        for path in project_paths.harmonized.rglob("*")
        if path.is_file()
    }
    second = pipeline.run([Stage.HARMONIZE])

    assert first.harmonized == 7
    assert second.harmonized == 0
    assert before == {
        path.relative_to(project_paths.dataset).as_posix(): sha256_file(path)
        for path in project_paths.harmonized.rglob("*")
        if path.is_file()
    }

    gpd.GeoDataFrame(
        {"HYRIV_ID": [1, 2]},
        geometry=[
            LineString([(102.95, 20.05), (103.25, 20.05)]),
            LineString([(103.00, 20.06), (103.20, 20.06)]),
        ],
        crs="EPSG:4326",
    ).to_file(rivers)
    river_record = catalog.get("raw-hydrorivers_v10-0")
    catalog.upsert(
        river_record.model_copy(
            update={"checksum": sha256_file(rivers), "size_bytes": rivers.stat().st_size}
        )
    )

    changed = pipeline.run([Stage.HARMONIZE])
    steady = pipeline.run([Stage.HARMONIZE])

    assert changed.harmonized == 7
    assert steady.harmonized == 0
