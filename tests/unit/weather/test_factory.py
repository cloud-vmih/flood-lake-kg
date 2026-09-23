import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from flashflood_data.orchestration.weather.factory import WeatherRuntime, _matches_planned_object
from flashflood_data.orchestration.weather.models import PlannedWeatherObject, WeatherWindow


def _planned() -> PlannedWeatherObject:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    return PlannedWeatherObject(
        source_id="era5_land",
        source_version="v2",
        stream_id="hourly_reanalysis",
        product="reanalysis-era5-land",
        asset_id="hourly_reanalysis-20260901T0000Z",
        window=WeatherWindow(start=start, end=start.replace(month=10)),
        variables=("total_precipitation",),
        request_fingerprint="f" * 64,
        source_cycle_id="20260901T0000Z",
        options={"aoi_bounds": (103.0, 20.0, 105.0, 22.0)},
    )


def _stored(planned: PlannedWeatherObject, **changes):
    selection = {
        "stream_id": planned.stream_id,
        "window_start": planned.window.start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": planned.window.end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "variables": list(planned.variables),
        "options": dict(planned.options),
    }
    selection.update(changes.pop("selection", {}))
    return SimpleNamespace(
        asset_id=planned.asset_id,
        source_version=changes.pop("source_version", planned.source_version),
        product=planned.product,
        source_type="dynamic",
        selection_json=json.dumps(selection),
        **changes,
    )


def test_inventory_coverage_requires_the_complete_expected_request_identity() -> None:
    planned = _planned()

    assert _matches_planned_object(_stored(planned), planned)
    assert not _matches_planned_object(_stored(planned, source_version="v1"), planned)
    assert not _matches_planned_object(
        _stored(planned, selection={"options": {"aoi_bounds": [103, 20, 106, 22]}}),
        planned,
    )


def test_provider_failure_is_recorded_before_raw_publication(tmp_path: Path, monkeypatch) -> None:
    class FailingProvider:
        def fetch(self, _planned, _target):
            raise TimeoutError("provider timed out")

    class MemoryMeta:
        def __init__(self) -> None:
            self.attempts = []

        def record_attempt(self, **values):
            self.attempts.append(values)

    meta = MemoryMeta()
    runtime = WeatherRuntime(
        root=tmp_path,
        config=SimpleNamespace(),
        settings=SimpleNamespace(staging_root=tmp_path),
        inventory=SimpleNamespace(),
        table_store=SimpleNamespace(),
        meta=meta,
        object_store=SimpleNamespace(),
    )
    monkeypatch.setattr(runtime, "provider", lambda _stream_id: FailingProvider())

    with pytest.raises(TimeoutError, match="provider timed out"):
        runtime.fetch(_planned(), "run-1", attempt_no=3)

    assert [item["status"] for item in meta.attempts] == ["running", "failed"]
    assert meta.attempts[-1]["attempt_no"] == 3
    assert meta.attempts[-1]["error_code"] == "TimeoutError"
