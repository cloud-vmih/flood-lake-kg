"""Unit tests for conservative historical-to-current commune mapping."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from flashflood_data.derive.mappings import (
    build_admin_crosswalk,
    normalize_admin_name,
    parse_predecessors,
)
from flashflood_data.sources.admin import normalize_historical_admin


@pytest.fixture
def historical_admin() -> gpd.GeoDataFrame:
    fixture = Path(__file__).parents[1] / "fixtures" / "admin" / "gadm_old_communes.geojson"
    return normalize_historical_admin(
        gpd.read_file(fixture), source_version="4.1", valid_to=date(2025, 6, 30), raw_asset_id="raw"
    )


@pytest.fixture
def current_admin() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "current_commune_code": ["03664"],
            "current_commune_name": ["Phường Chiềng An"],
            "predecessors_text": ["Phường Chiềng An, Xã Chiềng Xôm, Xã Chiềng Đen"],
        },
        geometry=[Polygon([(103, 21), (103.1, 21), (103.1, 21.1), (103, 21.1)])],
        crs="EPSG:4326",
    )


def test_normalize_admin_name_strips_prefix_and_normalizes_unicode_spacing() -> None:
    assert normalize_admin_name("  XÃ  Chiềng   Xôm ") == "chiềng xôm"


def test_parse_predecessors_keeps_the_written_name_and_type() -> None:
    parsed = parse_predecessors("Phường Chiềng An, Xã Chiềng Xôm")

    assert [(item.written_name, item.admin_type, item.normalized_name) for item in parsed] == [
        ("Phường Chiềng An", "Phường", "chiềng an"),
        ("Xã Chiềng Xôm", "Xã", "chiềng xôm"),
    ]


def test_crosswalk_maps_full_predecessors_by_name_and_overlap(
    current_admin: gpd.GeoDataFrame, historical_admin: gpd.GeoDataFrame
) -> None:
    result = build_admin_crosswalk(current_admin, historical_admin)
    old = result.loc[result.old_admin_name == "Chiềng Xôm"].iloc[0]
    unchanged = result.loc[result.old_admin_name == "Chiềng An"].iloc[0]

    assert old.current_commune_code == "03664"
    assert old.relationship_type == "merged"
    assert old.match_status == "matched"
    assert old.valid_to == date(2025, 6, 30)
    assert unchanged.relationship_type == "unchanged"
    assert json.loads(old.candidate_old_admin_ids) == ["VNM.14.1.2_1"]
    assert json.loads(old.overlap_metrics_json)[0]["overlap_fraction"] == pytest.approx(1.0)


def test_crosswalk_never_forces_one_old_unit_to_duplicate_current_targets(
    current_admin: gpd.GeoDataFrame, historical_admin: gpd.GeoDataFrame
) -> None:
    duplicate = current_admin.copy()
    duplicate.loc[1, "current_commune_code"] = "03665"
    duplicate.loc[1, "current_commune_name"] = "Xã Fixture"
    duplicate.loc[1, "predecessors_text"] = "Xã Chiềng Xôm"
    duplicate.loc[1, "geometry"] = duplicate.geometry.iloc[0]

    result = build_admin_crosswalk(duplicate, historical_admin)
    ambiguous = result.loc[result.old_admin_name == "Chiềng Xôm"]

    assert set(ambiguous.match_status) == {"ambiguous"}
    assert ambiguous.current_commune_code.isna().all()


def test_crosswalk_rejects_partial_merger_language_instead_of_inventing_a_fraction(
    current_admin: gpd.GeoDataFrame, historical_admin: gpd.GeoDataFrame
) -> None:
    current_admin.loc[0, "predecessors_text"] = "một phần Xã Chiềng Xôm"

    with pytest.raises(ValueError, match="một phần"):
        build_admin_crosswalk(current_admin, historical_admin)
