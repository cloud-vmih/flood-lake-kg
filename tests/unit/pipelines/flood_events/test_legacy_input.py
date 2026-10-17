"""Tests for immutable historical flood evidence parsing and valid-time replay."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from flashflood_data.pipelines.flood_events.legacy_input import (
    read_historical_events,
    resolve_event_administration,
)


@pytest.fixture
def fixture_workbook() -> Path:
    return Path(__file__).parents[3] / "fixtures" / "events" / "historical_events.xlsx"


@pytest.fixture
def current_admin() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "current_commune_code": ["03664"],
            "current_commune_name": ["Phường Chiềng An"],
        },
        geometry=[Polygon([(103, 21), (103.1, 21), (103.1, 21.1), (103, 21.1)])],
        crs="EPSG:4326",
    )


@pytest.fixture
def crosswalk() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "old_admin_name": ["Chiềng Xôm"],
            "old_admin_normalized_name": ["chiềng xôm"],
            "current_commune_code": ["03664"],
            "match_status": ["matched"],
            "valid_to": [date(2025, 6, 30)],
        }
    )


def test_workbook_reads_only_numbered_evidence_rows(fixture_workbook: Path) -> None:
    events = read_historical_events(fixture_workbook)

    assert events.event_id.tolist() == ["historical-flood:1", "historical-flood:2"]
    assert "original_place_text" in events
    assert events.loc[0, "event_date_start"] == date(2024, 6, 2)
    assert events.loc[0, "event_date_end"] == date(2024, 6, 3)


def test_pre_reform_event_uses_old_to_new_crosswalk(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    result = resolve_event_administration(
        read_historical_events(fixture_workbook), current_admin, crosswalk
    )
    old_event = result.loc[result.event_year == 2024].iloc[0]

    assert json.loads(old_event.current_commune_codes_json) == ["03664"]
    assert old_event.match_status == "matched"
    assert old_event.match_confidence == 1.0


def test_district_only_event_is_not_forced(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    result = resolve_event_administration(
        read_historical_events(fixture_workbook), current_admin, crosswalk
    )
    district_only = result.loc[result.original_place_text == "một số khu vực trong huyện"].iloc[0]

    assert district_only.match_status == "unresolved"
    assert district_only.current_commune_codes_json == "[]"


def test_pre_reform_old_marker_still_uses_historical_crosswalk(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    events = read_historical_events(fixture_workbook).iloc[[0]].copy()
    events.loc[:, "original_place_text"] = "Xã Chiềng Xôm (cũ)"

    result = resolve_event_administration(events, current_admin, crosswalk)

    assert result.loc[0, "match_status"] == "matched"
    assert json.loads(result.loc[0, "current_commune_codes_json"]) == ["03664"]


def test_multiple_predecessors_resolved_to_one_current_unit_are_matched(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    events = read_historical_events(fixture_workbook).iloc[[0]].copy()
    events.loc[:, "original_place_text"] = "Xã Chiềng Xôm; Xã Chiềng Đen"
    crosswalk = pd.concat(
        [
            crosswalk,
            pd.DataFrame(
                {
                    "old_admin_name": ["Chiềng Đen"],
                    "old_admin_normalized_name": ["chiềng đen"],
                    "current_commune_code": ["03664"],
                    "match_status": ["matched"],
                    "valid_to": [date(2025, 6, 30)],
                }
            ),
        ],
        ignore_index=True,
    )

    result = resolve_event_administration(events, current_admin, crosswalk)

    assert result.loc[0, "match_status"] == "matched"
    assert json.loads(result.loc[0, "current_commune_codes_json"]) == ["03664"]


def test_pre_reform_crosswalk_candidates_remain_ambiguous(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    crosswalk = crosswalk.assign(
        match_status="ambiguous",
        current_commune_code=None,
        candidate_current_commune_codes_json='["03664","03665"]',
    )

    result = resolve_event_administration(
        read_historical_events(fixture_workbook).iloc[[0]], current_admin, crosswalk
    )

    assert result.loc[0, "match_status"] == "ambiguous"
    assert json.loads(result.loc[0, "current_commune_codes_json"]) == ["03664", "03665"]


def test_pre_reform_ambiguous_name_without_codes_remains_ambiguous(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    crosswalk = crosswalk.assign(match_status="ambiguous", current_commune_code=None)

    result = resolve_event_administration(
        read_historical_events(fixture_workbook).iloc[[0]], current_admin, crosswalk
    )

    assert result.loc[0, "match_status"] == "ambiguous"
    assert result.loc[0, "match_confidence"] == 0.5
    assert result.loc[0, "current_commune_codes_json"] == "[]"


def test_post_reform_name_match_tolerates_admin_type_prefix_change(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    events = read_historical_events(fixture_workbook).iloc[[1]].copy()
    events.loc[:, "original_place_text"] = "Xã Chiềng An"

    result = resolve_event_administration(events, current_admin, crosswalk)

    assert result.loc[0, "match_status"] == "matched"
    assert json.loads(result.loc[0, "current_commune_codes_json"]) == ["03664"]
