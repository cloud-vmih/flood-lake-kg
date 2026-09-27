from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString, box

from flashflood_data.static.sources.hydro_fields import BASINATLAS_RAW_FIELDS
from flashflood_data.static.sources.hydro_subset import build_hydro_subset


def _write_aoi(tmp_path: Path) -> Path:
    path = tmp_path / "vietnam_hydrological_aoi.geoparquet"
    gpd.GeoDataFrame(
        {"aoi": ["vietnam_hydrological_aoi"]},
        geometry=[box(103.0, 20.0, 103.2, 20.2)],
        crs="EPSG:4326",
    ).to_parquet(path, index=False)
    return path


def _write_basins(tmp_path: Path) -> Path:
    path = tmp_path / "hydrobasins.shp"
    gpd.GeoDataFrame(
        {
            "HYBAS_ID": [102, 101, 999],
            "NEXT_DOWN": [101, 0, 0],
            "PFAF_ID": ["12", "11", "99"],
        },
        geometry=[
            box(103.1, 20.0, 103.2, 20.1),
            box(103.0, 20.0, 103.1, 20.1),
            box(110.0, 20.0, 110.1, 20.1),
        ],
        crs="EPSG:4326",
    ).to_file(path)
    return path


def test_hydrobasins_subset_is_sorted_and_records_selection(tmp_path: Path) -> None:
    result = build_hydro_subset(
        "hydrobasins_v1c", _write_basins(tmp_path), _write_aoi(tmp_path), tmp_path / "out"
    )

    layer = gpd.read_file(result.path)
    assert layer.HYBAS_ID.tolist() == [101, 102]
    assert result.source_feature_count == 3
    assert result.selected_feature_count == 2
    assert result.selected_hybas_ids == (101, 102)
    assert result.selection_version == "vietnam-l12-h1-v1"
    assert len(result.aoi_checksum) == 64
    assert result.path.with_suffix(".dbf").read_bytes()[1:4] == bytes((80, 1, 1))


def test_basinatlas_uses_hydrobasins_ids_and_allowlisted_fields(tmp_path: Path) -> None:
    values = {
        field: [101, 102, 999] if field == "HYBAS_ID" else [1, 2, 3]
        for field in BASINATLAS_RAW_FIELDS
    }
    values["unapproved"] = ["drop", "drop", "drop"]
    source = tmp_path / "basinatlas.gpkg"
    gpd.GeoDataFrame(
        values,
        geometry=[
            box(103.0, 20.0, 103.1, 20.1),
            box(103.1, 20.0, 103.2, 20.1),
            box(110.0, 20.0, 110.1, 20.1),
        ],
        crs="EPSG:4326",
    ).to_file(source, driver="GPKG")

    result = build_hydro_subset(
        "basinatlas_v10",
        source,
        _write_aoi(tmp_path),
        tmp_path / "out",
        selected_hybas_ids={101, 102},
    )

    layer = gpd.read_file(result.path)
    assert set(layer.HYBAS_ID) == {101, 102}
    assert set(layer.columns) == set(BASINATLAS_RAW_FIELDS) | {"geometry"}
    assert "unapproved" not in layer.columns


def test_hydrorivers_selects_intersections_without_clipping(tmp_path: Path) -> None:
    source = tmp_path / "rivers.gpkg"
    crossing = LineString([(102.9, 20.05), (103.3, 20.05)])
    outside = LineString([(110.0, 20.05), (110.2, 20.05)])
    gpd.GeoDataFrame(
        {"HYRIV_ID": [7, 8]}, geometry=[crossing, outside], crs="EPSG:4326"
    ).to_file(source, driver="GPKG")

    result = build_hydro_subset(
        "hydrorivers_v10", source, _write_aoi(tmp_path), tmp_path / "out"
    )

    layer = gpd.read_file(result.path)
    assert layer.HYRIV_ID.tolist() == [7]
    assert layer.geometry.iloc[0].equals(crossing)
    assert result.source_feature_count == 2
    assert result.selected_feature_count == 1


def test_subset_rejects_duplicate_feature_ids(tmp_path: Path) -> None:
    source = _write_basins(tmp_path)
    duplicate = gpd.read_file(source).iloc[[0, 0]].copy()
    duplicate.to_file(source)

    with pytest.raises(ValueError, match="duplicate HYBAS_ID"):
        build_hydro_subset(
            "hydrobasins_v1c", source, _write_aoi(tmp_path), tmp_path / "out"
        )
