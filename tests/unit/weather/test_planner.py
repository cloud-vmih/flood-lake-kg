from datetime import UTC, datetime

from flashflood_data.orchestration.weather.models import (
    IngestWatermark,
    WeatherStreamConfig,
)
from flashflood_data.orchestration.weather.planner import (
    advance_contiguous_cursor,
    operational_start,
    plan_expected_objects,
    provider_safe_end,
    select_missing_or_overlap,
)


def _stream(**updates: object) -> WeatherStreamConfig:
    values = {
        "stream_id": "hourly",
        "product": "rain",
        "start_at": datetime(2026, 1, 1, tzinfo=UTC),
        "step_minutes": 60,
        "chunk_minutes": 60,
        "availability_lag_minutes": 120,
        "overlap_steps": 2,
        "variables": ("precipitation",),
        "media_type": "application/json",
        "filename_suffix": ".json",
        "endpoint": "https://example.invalid/weather",
    }
    values.update(updates)
    return WeatherStreamConfig.model_validate(values)


def test_plans_stable_half_open_objects_until_provider_safe_end() -> None:
    objects = plan_expected_objects(
        source_id="rain_source",
        source_version="v1",
        stream=_stream(),
        start=datetime(2026, 1, 1, 0, tzinfo=UTC),
        end=datetime(2026, 1, 1, 3, tzinfo=UTC),
    )

    assert [item.window.start.hour for item in objects] == [0, 1, 2]
    assert [item.window.end.hour for item in objects] == [1, 2, 3]
    assert len({item.asset_id for item in objects}) == 3
    assert objects == plan_expected_objects(
        source_id="rain_source",
        source_version="v1",
        stream=_stream(),
        start=datetime(2026, 1, 1, 0, tzinfo=UTC),
        end=datetime(2026, 1, 1, 3, tzinfo=UTC),
    )


def test_request_options_are_part_of_the_expected_identity() -> None:
    kwargs = {
        "source_id": "era5_land",
        "source_version": "v1",
        "stream": _stream(),
        "start": datetime(2026, 1, 1, 0, tzinfo=UTC),
        "end": datetime(2026, 1, 1, 1, tzinfo=UTC),
    }
    first = plan_expected_objects(
        **kwargs, request_options={"aoi_bounds": [103.0, 20.0, 105.0, 22.0]}
    )[0]
    changed = plan_expected_objects(
        **kwargs, request_options={"aoi_bounds": [103.0, 20.0, 106.0, 22.0]}
    )[0]

    assert first.options["aoi_bounds"] == (103.0, 20.0, 105.0, 22.0)
    assert first.request_fingerprint != changed.request_fingerprint


def test_fixed_windows_follow_request_cadence_and_may_overlap() -> None:
    stream = _stream(step_minutes=30, chunk_minutes=60)
    objects = plan_expected_objects(
        source_id="gsmap",
        source_version="v8",
        stream=stream,
        start=datetime(2026, 1, 1, 0, tzinfo=UTC),
        end=datetime(2026, 1, 1, 2, tzinfo=UTC),
    )

    assert [(item.window.start.minute, item.window.end.hour, item.window.end.minute) for item in objects] == [
        (0, 1, 0),
        (30, 1, 30),
        (0, 2, 0),
    ]
    assert advance_contiguous_cursor(
        objects, {item.asset_id: "available" for item in objects}
    ) == datetime(2026, 1, 1, 2, tzinfo=UTC)


def test_existing_objects_are_skipped_except_inside_overlap() -> None:
    expected = plan_expected_objects(
        source_id="rain_source",
        source_version="v1",
        stream=_stream(overlap_steps=1),
        start=datetime(2026, 1, 1, 0, tzinfo=UTC),
        end=datetime(2026, 1, 1, 3, tzinfo=UTC),
    )

    selected = select_missing_or_overlap(
        expected,
        existing_asset_ids={item.asset_id for item in expected},
        overlap_start=datetime(2026, 1, 1, 2, tzinfo=UTC),
    )

    assert [item.window.start.hour for item in selected] == [2]


def test_overlap_does_not_redownload_object_committed_after_cursor_update() -> None:
    expected = plan_expected_objects(
        source_id="rain_source",
        source_version="v1",
        stream=_stream(overlap_steps=1),
        start=datetime(2026, 1, 1, 0, tzinfo=UTC),
        end=datetime(2026, 1, 1, 3, tzinfo=UTC),
    )

    selected = select_missing_or_overlap(
        expected,
        existing_asset_ids={item.asset_id for item in expected},
        overlap_start=datetime(2026, 1, 1, 2, tzinfo=UTC),
        recently_committed_asset_ids={expected[2].asset_id},
    )

    assert selected == ()


def test_cursor_stops_before_gap_and_accepts_explicit_no_data() -> None:
    expected = plan_expected_objects(
        source_id="rain_source",
        source_version="v1",
        stream=_stream(),
        start=datetime(2026, 1, 1, 10, tzinfo=UTC),
        end=datetime(2026, 1, 1, 13, tzinfo=UTC),
    )
    status = {
        expected[0].asset_id: "available",
        expected[2].asset_id: "available",
    }
    assert advance_contiguous_cursor(expected, status) == datetime(
        2026, 1, 1, 11, tzinfo=UTC
    )

    status[expected[1].asset_id] = "no_data"
    assert advance_contiguous_cursor(expected, status) == datetime(
        2026, 1, 1, 13, tzinfo=UTC
    )


def test_operational_start_replays_watermark_overlap() -> None:
    stream = _stream(overlap_steps=2)
    watermark = IngestWatermark(
        source_id="rain_source",
        product="rain",
        stream_id="hourly",
        cursor_time=datetime(2026, 1, 2, 0, tzinfo=UTC),
        last_safe_end=datetime(2026, 1, 2, 0, tzinfo=UTC),
        last_run_id="old",
        status="ready",
        updated_at=datetime(2026, 1, 2, 0, tzinfo=UTC),
    )

    assert operational_start(stream, watermark) == datetime(2026, 1, 1, 22, tzinfo=UTC)


def test_calendar_month_operational_bounds_stay_on_complete_months() -> None:
    stream = _stream(
        chunking="calendar_month",
        chunk_minutes=44640,
        start_at=datetime(2024, 1, 1, tzinfo=UTC),
        availability_lag_minutes=14400,
        overlap_steps=120,
    )
    watermark = IngestWatermark(
        source_id="era5_land",
        product="reanalysis-era5-land",
        stream_id="hourly",
        cursor_time=datetime(2026, 9, 1, tzinfo=UTC),
        last_safe_end=datetime(2026, 9, 1, tzinfo=UTC),
        last_run_id="old",
        status="ready",
        updated_at=datetime(2026, 9, 1, tzinfo=UTC),
    )

    assert operational_start(stream, watermark) == datetime(2026, 8, 1, tzinfo=UTC)
    assert provider_safe_end(
        datetime(2026, 9, 23, 12, tzinfo=UTC), stream
    ) == datetime(2026, 9, 1, tzinfo=UTC)


def test_calendar_month_chunking_never_cross_multiplies_dates() -> None:
    stream = _stream(
        chunking="calendar_month",
        chunk_minutes=44640,
        start_at=datetime(2024, 1, 1, tzinfo=UTC),
    )

    objects = plan_expected_objects(
        source_id="era5_land",
        source_version="v1",
        stream=stream,
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 4, 1, tzinfo=UTC),
    )

    assert [(item.window.start.day, item.window.start.month, item.window.end.day, item.window.end.month) for item in objects] == [
        (1, 1, 1, 2),
        (1, 2, 1, 3),
        (1, 3, 1, 4),
    ]
