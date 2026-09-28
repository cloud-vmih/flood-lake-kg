import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashflood_data.orchestration.landing.models import RegisteredBatch
from flashflood_data.orchestration.weather.landing import WeatherLandingService
from flashflood_data.orchestration.weather.lifecycle import ObjectLifecycleStore
from flashflood_data.orchestration.weather.models import (
    PlannedWeatherObject,
    ScopedWeatherObject,
    WeatherWindow,
)
from flashflood_data.storage.object_store import ObjectPublisher


class MemoryObjectStore:
    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.data

    def size(self, key: str) -> int:
        return len(self.data[key])

    def sha256(self, key: str) -> str:
        from hashlib import sha256

        return sha256(self.data[key]).hexdigest()

    def read(self, key: str) -> bytes:
        return self.data[key]

    def upload(self, path: Path, key: str, metadata: Mapping[str, str]) -> None:
        self.data[key] = path.read_bytes()

    def copy(self, source_key: str, destination_key: str) -> None:
        self.data[destination_key] = self.data[source_key]

    def delete(self, key: str) -> None:
        self.data.pop(key, None)


class MemoryInventory:
    def __init__(self) -> None:
        self.rows = {}

    def register_many(self, rows):
        reused = 0
        for row in rows:
            if row.object_id in self.rows:
                reused += 1
            self.rows[row.object_id] = row
        return RegisteredBatch(
            source_id=rows[0].source_id,
            run_id=rows[0].ingest_run_id,
            object_ids=tuple(row.object_id for row in rows),
            snapshot_id=9,
            reused=reused,
        )


class MemoryMeta:
    def __init__(self) -> None:
        self.attempts = []

    def record_attempt(self, **values):
        self.attempts.append(values)
        return 1


class MemoryLifecycleBackend:
    def __init__(self) -> None:
        self.rows = {}

    def get_meta_row(self, identifier, key):
        assert identifier == ("meta", "object_lifecycle")
        return self.rows.get(key["object_id"])

    def get_keyed_rows(self, identifier, key):
        assert identifier == ("meta", "object_lifecycle")
        return [
            row for row in self.rows.values() if all(row[name] == value for name, value in key.items())
        ]

    def upsert_meta_row(self, identifier, key_fields, row):
        assert identifier == ("meta", "object_lifecycle")
        assert key_fields == ("object_id",)
        self.rows[row["object_id"]] = dict(row)
        return 13


def _scoped(tmp_path: Path, payload: bytes = b"weather") -> ScopedWeatherObject:
    from hashlib import sha256

    path = tmp_path / "rain.json"
    path.write_bytes(payload)
    start = datetime(2026, 9, 1, 0, tzinfo=UTC)
    planned = PlannedWeatherObject(
        source_id="rain_source",
        source_version="v1",
        spatial_scope_id="sonla-l12-h1-deadbeef",
        stream_id="hourly",
        product="rain",
        asset_id="hourly-20260901T0000Z",
        window=WeatherWindow(start=start, end=start.replace(hour=1)),
        variables=("precipitation",),
        request_fingerprint="f" * 64,
        source_cycle_id="20260901T0000Z",
    )
    return ScopedWeatherObject(
        planned=planned,
        path=path,
        filename=path.name,
        media_type="application/json",
        source_uri="https://example.test/rain.json",
        retrieved_at=start.replace(hour=2),
        available_at=start.replace(hour=1),
        provider_metadata={"provider": "fixture"},
        spatial_scope_id="sonla-l12-h1-deadbeef",
        source_grid_version="grid-v1",
        cell_indices=(101, 102),
        provider_payload_checksum="a" * 64,
        provider_payload_size_bytes=10_000,
        scoped_payload_checksum=sha256(payload).hexdigest(),
        scoped_payload_size_bytes=len(payload),
    )


def _service(object_store, inventory, meta, lifecycle_backend, retention_class="durable"):
    return WeatherLandingService(
        publisher=ObjectPublisher(object_store, "raw"),
        inventory=inventory,
        meta=meta,
        lifecycle=ObjectLifecycleStore(lifecycle_backend),
        retention_class=retention_class,
        license_id="provider-terms",
    )


def test_publish_registers_dynamic_raw_payload_and_credential_free_manifest(tmp_path: Path) -> None:
    object_store = MemoryObjectStore()
    inventory = MemoryInventory()
    meta = MemoryMeta()
    lifecycle = MemoryLifecycleBackend()
    service = _service(object_store, inventory, meta, lifecycle)

    result = service.publish_and_register(_scoped(tmp_path), run_id="run-1")

    assert result.status == "available"
    assert result.snapshot_id == 9
    row = inventory.rows[result.object_id]
    assert row.source_type == "dynamic"
    assert row.product == "rain"
    assert row.valid_time == "2026-09-01T00:00:00Z"
    assert row.object_uri.startswith("s3://raw/weather/rain_source/rain/")
    manifest_key = row.manifest_uri.removeprefix("s3://")
    manifest = json.loads(object_store.data[manifest_key])
    assert manifest["request_fingerprint"] == "f" * 64
    assert manifest["selection"]["spatial_scope_id"] == "sonla-l12-h1-deadbeef"
    assert manifest["selection"]["cell_count"] == 2
    assert manifest["provider_metadata"]["provider_payload_checksum"] == "a" * 64
    assert "password" not in json.dumps(manifest).lower()
    assert meta.attempts[-1]["status"] == "succeeded"
    assert not (tmp_path / "rain.json").exists()
    assert lifecycle.rows[result.object_id]["retention_class"] == "durable"
    assert lifecycle.rows[result.object_id]["expires_at"] is None


def test_rerun_reuses_identical_raw_object_without_duplicate_inventory(tmp_path: Path) -> None:
    object_store = MemoryObjectStore()
    inventory = MemoryInventory()
    lifecycle = MemoryLifecycleBackend()
    service = _service(object_store, inventory, MemoryMeta(), lifecycle)

    first = service.publish_and_register(_scoped(tmp_path), run_id="run-1")
    second = service.publish_and_register(_scoped(tmp_path), run_id="run-2")

    assert first.object_id == second.object_id
    assert second.reused is True
    assert len(inventory.rows) == 1
    assert len(lifecycle.rows) == 1


def test_revised_payload_gets_a_new_immutable_object(tmp_path: Path) -> None:
    object_store = MemoryObjectStore()
    inventory = MemoryInventory()
    service = _service(
        object_store, inventory, MemoryMeta(), MemoryLifecycleBackend()
    )

    first = service.publish_and_register(_scoped(tmp_path, b"old"), run_id="run-1")
    second = service.publish_and_register(_scoped(tmp_path, b"revised"), run_id="run-2")

    assert first.object_id != second.object_id
    assert len(inventory.rows) == 2


def test_staging_read_failure_is_audited(tmp_path: Path) -> None:
    meta = MemoryMeta()
    service = _service(
        MemoryObjectStore(),
        MemoryInventory(),
        meta,
        MemoryLifecycleBackend(),
    )
    scoped = _scoped(tmp_path)
    scoped.path.unlink()

    with pytest.raises(FileNotFoundError):
        service.publish_and_register(scoped, run_id="run-1", attempt_no=2)

    assert meta.attempts[-1]["status"] == "failed"
    assert meta.attempts[-1]["attempt_no"] == 2


def test_now_publication_registers_seven_day_lifecycle(tmp_path: Path) -> None:
    lifecycle = MemoryLifecycleBackend()
    service = _service(
        MemoryObjectStore(),
        MemoryInventory(),
        MemoryMeta(),
        lifecycle,
        retention_class="transient_7d",
    )
    scoped = _scoped(tmp_path)

    published = service.publish_and_register(scoped, run_id="run-1")

    row = lifecycle.rows[published.object_id]
    assert row["expires_at"] == scoped.retrieved_at.replace(day=8)
    assert row["storage_status"] == "available"
