"""The single supported AOI build uses HydroBASINS L12."""

from pathlib import Path

import geopandas as gpd
from shapely.geometry import box

from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.workflow import Stage, StaticPipeline


def test_lakehouse_aoi_reads_l12_without_l10(project_paths: ProjectPaths) -> None:
    root = project_paths.root
    config = root / "config"
    config.mkdir()
    (config / "study_area.yaml").write_text(
        "hydrobasins_level: 12\nupstream_hops: 1\n", encoding="utf-8"
    )
    basin_dir = project_paths.dataset / "hybas_as_lev01-12_v1c"
    basin_dir.mkdir()
    fixture = Path(__file__).parents[2] / "fixtures" / "hydro" / "basins_l10.geojson"
    gpd.read_file(fixture).to_file(basin_dir / "hybas_as_lev12_v1c.shp")
    aoi_dir = project_paths.harmonized / "aoi"
    admin_dir = project_paths.harmonized / "admin"
    aoi_dir.mkdir(parents=True)
    admin_dir.mkdir(parents=True)
    gpd.GeoDataFrame(geometry=[box(103.01, 20.01, 103.09, 20.09)], crs="EPSG:4326").to_parquet(
        aoi_dir / "core_aoi.geoparquet", index=False
    )
    gpd.GeoDataFrame(geometry=[box(103, 20, 103.15, 20.15)], crs="EPSG:4326").to_parquet(
        admin_dir / "vietnam_boundary.geoparquet", index=False
    )

    pipeline = StaticPipeline(project_paths, source_specs={}, stage_handlers={})
    summary = pipeline.run([Stage.AOI])

    assert summary.status == "completed"
    assert summary.metrics["selected_l12"] == 2
    assert (aoi_dir / "environmental_aoi.geoparquet").is_file()
    assert (aoi_dir / "hydrological_aoi.geoparquet").is_file()


def test_make_has_only_l12_aoi_target() -> None:
    root = Path(__file__).parents[3]
    makefile = (root / "Makefile").read_text()
    assert "lakehouse-aoi:" in makefile
    assert "--stop-after aoi" in makefile
    assert "preflight-aoi:" not in makefile
    assert "--aoi-level" not in makefile
    assert "hydrobasins_level: 12" in (root / "config/study_area.yaml").read_text()
