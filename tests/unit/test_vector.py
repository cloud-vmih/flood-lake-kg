"""Unit tests for shared vector validation and persistence."""

from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from flashflood_data.config import StudyAreaConfig
from flashflood_data.static.sources.admin import validate_current_admin
from flashflood_data.static.spatial.vector import (
    repair_geometries,
    validate_vector,
    write_geoparquet,
)


def test_repair_geometries_marks_repaired_bow_tie() -> None:
    bow_tie = Polygon([(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)])
    source = gpd.GeoDataFrame({"id": ["x"]}, geometry=[bow_tie], crs="EPSG:4326")

    repaired = repair_geometries(source)

    assert repaired.geometry.iloc[0].is_valid
    assert repaired.geometry_repaired.tolist() == [True]
    assert repaired.geometry_was_valid.tolist() == [False]
    assert source.geometry.iloc[0].is_valid is False


def test_validate_vector_rejects_wrong_crs_and_missing_required_column() -> None:
    frame = gpd.GeoDataFrame({"name": ["fixture"]}, geometry=[Polygon([(0, 0), (1, 0), (1, 1)])], crs="EPSG:3857")

    result = validate_vector(frame, required_columns=("code",), expected_crs="EPSG:4326")

    assert result.passed is False
    assert result.checks["required_columns"] is False
    assert result.checks["expected_crs"] is False


def test_write_geoparquet_reprojects_without_mutating_source(tmp_path) -> None:
    source = gpd.GeoDataFrame(
        {"name": ["fixture"]}, geometry=[Polygon([(0, 0), (1, 0), (1, 1)])], crs="EPSG:3857"
    )
    target = tmp_path / "vectors" / "fixture.geoparquet"

    written = write_geoparquet(source, target)

    reread = gpd.read_parquet(written)
    assert written == target
    assert str(source.crs) == "EPSG:3857"
    assert reread.crs.to_epsg() == 4326


@pytest.mark.parametrize("count", [74, 76])
def test_current_admin_rejects_any_count_other_than_75(count: int) -> None:
    frame = gpd.GeoDataFrame(
        {
            "current_commune_code": [f"{item:05d}" for item in range(count)],
            "current_commune_name": ["Xã Fixture"] * count,
            "unit_type": ["commune"] * count,
            "legal_area_km2": [1.0] * count,
            "lookup_id": [f"sonla-admin-2025:{item:05d}" for item in range(count)],
            "valid_from": ["2025-07-01"] * count,
            "raw_asset_id": [f"sonla-admin-2025-unit-{item:05d}" for item in range(count)],
        },
        geometry=[Polygon([(103, 21), (103.01, 21), (103.01, 21.01)])] * count,
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="expected 75"):
        validate_current_admin(frame, StudyAreaConfig())


def test_current_admin_requires_67_communes_and_8_wards() -> None:
    count = 75
    frame = gpd.GeoDataFrame(
        {
            "current_commune_code": [f"{item:05d}" for item in range(count)],
            "current_commune_name": ["Xã Fixture"] * count,
            "unit_type": ["commune"] * count,
            "legal_area_km2": [1.0] * count,
            "lookup_id": [f"sonla-admin-2025:{item:05d}" for item in range(count)],
            "valid_from": ["2025-07-01"] * count,
            "raw_asset_id": [f"sonla-admin-2025-unit-{item:05d}" for item in range(count)],
        },
        geometry=[Polygon([(103, 21), (103.01, 21), (103.01, 21.01)])] * count,
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="expected 67 communes and 8 wards"):
        validate_current_admin(frame, StudyAreaConfig())
