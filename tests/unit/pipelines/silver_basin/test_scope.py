from pathlib import Path

import geopandas as gpd
from shapely.geometry import box


def test_sonla_scope_uses_basin_interiors_not_shared_boundaries(tmp_path: Path) -> None:
    from flashflood_data.pipelines.silver_basin.scope import read_basin_scope

    source_dir = tmp_path / "dataset" / "hybas_as_lev01-12_v1c"
    source_dir.mkdir(parents=True)
    source = source_dir / "hybas_as_lev12_v1c.shp"
    gpd.GeoDataFrame(
        {"HYBAS_ID": [101, 102, 103]},
        geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1), box(2, 0, 3, 1)],
        crs="EPSG:4326",
    ).to_file(source)
    aoi_path = tmp_path / "dataset" / "harmonized" / "aoi" / "hydrological_aoi.geoparquet"
    aoi_path.parent.mkdir(parents=True)
    gpd.GeoDataFrame(geometry=[box(0, 0, 2, 1)], crs="EPSG:4326").to_parquet(aoi_path)

    scope = read_basin_scope(tmp_path, aoi_path.relative_to(tmp_path), level=12)

    assert scope.basin_ids == ("101", "102")
    assert len(scope.aoi_checksum) == 64


def test_sonla_scope_rejects_empty_selection(tmp_path: Path) -> None:
    import pytest

    from flashflood_data.pipelines.silver_basin.scope import read_basin_scope

    source_dir = tmp_path / "dataset" / "hybas_as_lev01-12_v1c"
    source_dir.mkdir(parents=True)
    gpd.GeoDataFrame({"HYBAS_ID": [101]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326").to_file(
        source_dir / "hybas_as_lev12_v1c.shp"
    )
    aoi_path = tmp_path / "dataset" / "harmonized" / "aoi" / "hydrological_aoi.geoparquet"
    aoi_path.parent.mkdir(parents=True)
    gpd.GeoDataFrame(geometry=[box(5, 5, 6, 6)], crs="EPSG:4326").to_parquet(aoi_path)

    with pytest.raises(ValueError, match="no basin interiors"):
        read_basin_scope(tmp_path, aoi_path.relative_to(tmp_path), level=12)
