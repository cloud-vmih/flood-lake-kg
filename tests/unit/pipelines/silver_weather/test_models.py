"""Domain contracts for versioned grid weights and basin weather values."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from flashflood_data.pipelines.silver_weather.models import (
    BasinWeatherValueRow,
    GridBasinWeightRow,
)

UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10.


def _weight(**changes: object) -> GridBasinWeightRow:
    data = {
        "source_id": "gsmap",
        "source_grid_version": "grid-v1",
        "source_grid_id": "cell-7",
        "basin_id": "4120890010",
        "basin_version": "basin-v1",
        "geometry_processing_version": "overlay-v1",
        "intersection_area_m2": 100.0,
        "weight_by_basin": 0.25,
        "weight_by_grid": 0.5,
        "quality_status": "passed",
    }
    data.update(changes)
    return GridBasinWeightRow.model_validate(data)


def _weather(**changes: object) -> BasinWeatherValueRow:
    data = {
        "business_key_hash": "a" * 64,
        "basin_id": "4120890010",
        "basin_version": "basin-v1",
        "source_id": "gsmap",
        "source_product": "gauge_standard_v8",
        "source_grid_version": "grid-v1",
        "source_slice_id": "slice-1",
        "source_cycle_id": "standard",
        "model_run_time": None,
        "valid_time": datetime(2020, 1, 1, 1, tzinfo=UTC),
        "window_start": datetime(2020, 1, 1, tzinfo=UTC),
        "window_end": datetime(2020, 1, 1, 1, tzinfo=UTC),
        "variable": "precipitation",
        "vertical_level": "surface",
        "value": 0.0,
        "unit": "mm",
        "value_kind": "interval_sum",
        "source_revision": 0,
        "available_at": datetime(2020, 1, 4, tzinfo=UTC),
        "valid_coverage_fraction": 0.8,
        "quality_flags_json": "{}",
    }
    data.update(changes)
    return BasinWeatherValueRow.model_validate(data)


def test_grid_weight_rejects_zero_area_and_out_of_range_fraction() -> None:
    assert _weight().weight_by_basin == 0.25
    with pytest.raises(ValidationError, match="intersection_area_m2"):
        _weight(intersection_area_m2=0)
    with pytest.raises(ValidationError, match="weight_by_grid"):
        _weight(weight_by_grid=1.2)


def test_basin_value_preserves_zero_rain_and_rejects_invalid_coverage() -> None:
    assert _weather().value == 0.0
    with pytest.raises(ValidationError, match="valid_coverage_fraction"):
        _weather(valid_coverage_fraction=1.1)


def test_basin_value_requires_utc_ordered_windows_and_json_flags() -> None:
    with pytest.raises(ValidationError, match="UTC"):
        _weather(window_start=datetime(2020, 1, 1, tzinfo=UTC).replace(tzinfo=None))
    with pytest.raises(ValidationError, match="window_end"):
        _weather(window_end=datetime(2020, 1, 1, tzinfo=UTC))
    with pytest.raises(ValidationError, match="quality_flags_json"):
        _weather(quality_flags_json="not-json")
    with pytest.raises(ValidationError, match="quality_flags_json"):
        _weather(quality_flags_json="[]")


def test_instantaneous_soil_moisture_can_use_a_point_in_time_window() -> None:
    instant = datetime(2020, 1, 1, 1, tzinfo=UTC)
    assert _weather(
        variable="soil_moisture",
        value_kind="instantaneous",
        unit="m3/m3",
        window_start=instant,
        window_end=instant,
    ).window_end == instant
