import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.orchestration.weather.config import load_weather_config
from flashflood_data.orchestration.weather.factory import WeatherRuntime, _matches_planned_object
from flashflood_data.orchestration.weather.models import PlannedWeatherObject, WeatherWindow


def _planned() -> PlannedWeatherObject:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    return PlannedWeatherObject(
        source_id="era5_land",
        source_version="v2",
        spatial_scope_id="sonla-scope-v1",
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
        "spatial_scope_id": planned.spatial_scope_id,
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


def test_meta_registration_targets_weather_raster_slice_contract() -> None:
    class MemoryMeta:
        def __init__(self) -> None:
            self.datasets = []

        def register_source(self, row):
            return 1

        def register_dataset(self, row):
            self.datasets.append(row)
            return 2

    meta = MemoryMeta()
    runtime = WeatherRuntime(
        root=Path(__file__).resolve().parents[3],
        config=load_weather_config(
            Path(__file__).resolve().parents[3] / "config/dynamic/gsmap_standard.yaml"
        ),
        settings=SimpleNamespace(),
        inventory=SimpleNamespace(),
        table_store=SimpleNamespace(),
        meta=meta,
        object_store=SimpleNamespace(),
    )

    runtime.register_meta()

    assert meta.datasets[0]["dataset_id"] == "flood_lakehouse.bronze.weather_raster_slice"
    assert meta.datasets[0]["schema_ref"].endswith("#bronzeweather_raster_slice")


def test_transient_cleanup_confirms_raw_delete_before_marking_expired() -> None:
    class LifecycleBackend:
        def __init__(self) -> None:
            self.rows = {}

        def get_meta_row(self, _identifier, key):
            return self.rows.get(key["object_id"])

        def get_keyed_rows(self, _identifier, key):
            return [
                row
                for row in self.rows.values()
                if all(row[name] == value for name, value in key.items())
            ]

        def upsert_meta_row(self, _identifier, _key_fields, row):
            self.rows[row["object_id"]] = dict(row)
            return 1

    class MemoryObjectStore:
        def __init__(self) -> None:
            self.keys = {"raw/weather/object-1.json"}

        def delete(self, key):
            self.keys.discard(key)

        def exists(self, key):
            return key in self.keys

    from flashflood_data.orchestration.weather.lifecycle import ObjectLifecycleStore

    backend = LifecycleBackend()
    lifecycle = ObjectLifecycleStore(backend)
    published = datetime(2026, 9, 1, tzinfo=UTC)
    lifecycle.register("object-1", "transient_7d", published, "landing-run")
    lifecycle.mark_bronze_evidence(
        "object-1", 42, "passed", "edge-1", published.replace(day=2)
    )
    row = SourceObjectRow(
        object_id="object-1",
        asset_id="asset-1",
        source_id="gsmap",
        source_version="v8",
        source_type="dynamic",
        product="gauge_now_v8",
        object_uri="s3://raw/weather/object-1.json",
        manifest_uri="s3://raw/weather/object-1.json.manifest.json",
        media_type="application/json",
        size_bytes=1,
        checksum="a" * 64,
        source_uri="https://example.test",
        retrieved_at=published,
        first_seen_at=published,
        ingest_run_id="landing-run",
    )
    object_store = MemoryObjectStore()
    runtime = WeatherRuntime(
        root=Path.cwd(),
        config=SimpleNamespace(source_id="gsmap"),
        settings=SimpleNamespace(raw_bucket="raw"),
        inventory=SimpleNamespace(available_objects=lambda _source_id: (row,)),
        table_store=backend,
        meta=SimpleNamespace(),
        object_store=object_store,
    )

    result = runtime.expire_transient_raw(datetime(2026, 9, 9, tzinfo=UTC))

    assert result["expired"] == ["object-1"]
    assert not object_store.keys
    assert backend.rows["object-1"]["storage_status"] == "expired"
