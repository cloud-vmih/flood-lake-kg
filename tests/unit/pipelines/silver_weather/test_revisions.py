"""Silver weather identity keeps sources, windows, and revisions distinct."""

from datetime import datetime, timezone

import pytest

from flashflood_data.pipelines.silver_weather.revisions import (
    weather_business_key,
    weather_business_key_sql,
)

UTC = timezone.utc  # noqa: UP017 - Spark runtime uses Python 3.10.


def _row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "basin_id": "4120890010",
        "basin_version": "basin-v1",
        "source_id": "gsmap",
        "source_product": "gauge_now_v8",
        "source_grid_version": "grid-v1",
        "source_cycle_id": "20200101T1000Z",
        "model_run_time": None,
        "valid_time": datetime(2020, 1, 1, 10, tzinfo=UTC),
        "window_start": datetime(2020, 1, 1, 9, tzinfo=UTC),
        "window_end": datetime(2020, 1, 1, 10, tzinfo=UTC),
        "variable": "precipitation",
        "vertical_level": "surface",
        "source_revision": 0,
    }
    row.update(changes)
    return row


def test_business_key_is_deterministic_and_ignores_quality_and_slice_id() -> None:
    original = _row()
    changed = {**original, "source_slice_id": "other-slice", "quality_flags_json": "{}"}

    assert weather_business_key(original) == weather_business_key(changed)
    assert len(weather_business_key(original)) == 64


def test_business_key_preserves_now_standard_shifted_window_and_revision() -> None:
    original = weather_business_key(_row())

    assert weather_business_key(_row(source_product="gauge_standard_v8")) != original
    assert weather_business_key(
        _row(
            window_start=datetime(2020, 1, 1, 9, 30, tzinfo=UTC),
            window_end=datetime(2020, 1, 1, 10, 30, tzinfo=UTC),
        )
    ) != original
    assert weather_business_key(_row(source_revision=1)) != original
    assert weather_business_key(_row(source_id="era5_land")) != original


def test_business_key_rejects_missing_identity_and_non_utc_time() -> None:
    with pytest.raises(ValueError, match="basin_id"):
        weather_business_key(_row(basin_id=""))
    with pytest.raises(ValueError, match="UTC"):
        weather_business_key(_row(valid_time=datetime(2020, 1, 1, 10, tzinfo=UTC).replace(tzinfo=None)))
    with pytest.raises(ValueError, match="source_revision"):
        weather_business_key(_row(source_revision=-1))


def test_spark_hash_expression_uses_same_identity_fields_and_nullable_run_time() -> None:
    expression = weather_business_key_sql("w")

    assert "sha2(to_json(named_struct(" in expression
    assert "unix_micros(w.valid_time)" in expression
    assert "unix_micros(w.model_run_time)" in expression
    assert "ignoreNullFields" in expression
    assert "source_slice_id" not in expression
    assert "quality_flags_json" not in expression
    with pytest.raises(ValueError, match="alias"):
        weather_business_key_sql("w; DROP TABLE silver")
