"""Tests for immutable historical flood evidence parsing and valid-time replay."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.models import AssetKind, AssetRecord, AssetStatus, SourceSpec
from flashflood_data.paths import ProjectPaths
from flashflood_data.static.harmonize.exposure import (
    read_historical_events,
    resolve_event_administration,
)
from flashflood_data.static.sources.base import SourceContext
from flashflood_data.static.sources.existing import ExistingAdapter


@pytest.fixture
def fixture_workbook() -> Path:
    return Path(__file__).parents[1] / "fixtures" / "events" / "historical_events.xlsx"


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
    result = resolve_event_administration(read_historical_events(fixture_workbook), current_admin, crosswalk)
    old_event = result.loc[result.event_year == 2024].iloc[0]

    assert json.loads(old_event.current_commune_codes_json) == ["03664"]
    assert old_event.match_status == "matched"
    assert old_event.match_confidence == 1.0


def test_district_only_event_is_not_forced(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    result = resolve_event_administration(read_historical_events(fixture_workbook), current_admin, crosswalk)
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

    result = resolve_event_administration(read_historical_events(fixture_workbook).iloc[[0]], current_admin, crosswalk)

    assert result.loc[0, "match_status"] == "ambiguous"
    assert json.loads(result.loc[0, "current_commune_codes_json"]) == ["03664", "03665"]


def test_pre_reform_ambiguous_name_without_codes_remains_ambiguous(
    fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    crosswalk = crosswalk.assign(match_status="ambiguous", current_commune_code=None)

    result = resolve_event_administration(read_historical_events(fixture_workbook).iloc[[0]], current_admin, crosswalk)

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


def test_existing_adapter_writes_evidence_table_without_static_feature_labels(
    tmp_path: Path, fixture_workbook: Path, current_admin: gpd.GeoDataFrame, crosswalk: pd.DataFrame
) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    current_path = paths.harmonized / "admin" / "admin_commune_2025.geoparquet"
    current_path.parent.mkdir(parents=True)
    current_admin.to_parquet(current_path, index=False)
    crosswalk_path = paths.derived / "mappings" / "admin_commune_crosswalk.parquet"
    crosswalk_path.parent.mkdir(parents=True)
    crosswalk.to_parquet(crosswalk_path, index=False)
    context = SourceContext(
        paths=paths,
        catalog=AssetCatalog(paths),
        study_area=StudyAreaConfig(),
        environment=EnvironmentSettings(_env_file=None),
        run_id="historical-event-integration-test",
    )
    raw = AssetRecord(
        asset_id="historical-events-raw",
        source_id="historical_flood_evidence_2020_2026",
        source_version="2026-08-14",
        kind=AssetKind.RAW,
        source_uri=fixture_workbook.as_uri(),
        storage_path=str(fixture_workbook),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        size_bytes=fixture_workbook.stat().st_size,
        checksum=sha256_file(fixture_workbook),
        retrieved_at=pd.Timestamp("2026-08-23", tz="UTC").to_pydatetime(),
        license_id="project-evidence-compilation",
        pipeline_run_id=context.run_id,
        status=AssetStatus.VALIDATED,
    )
    adapter = ExistingAdapter(
        SourceSpec(
            source_id="historical_flood_evidence_2020_2026",
            adapter="existing",
            version="2026-08-14",
            license_id="project-evidence-compilation",
        )
    )

    output = adapter.harmonize(context, [raw])[0]

    result = pd.read_parquet(output.storage_path)
    assert result["match_status"].tolist() == ["matched", "unresolved"]
    assert json.loads(output.metadata_json)["event_labels_used_as_static_features"] is False
