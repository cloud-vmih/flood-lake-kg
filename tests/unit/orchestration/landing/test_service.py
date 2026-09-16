from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from flashflood_data.orchestration.landing.config import (
    LandingSourcePolicy,
    StaticLandingConfig,
)
from flashflood_data.orchestration.landing.models import PreparedObject, RegisteredBatch
from flashflood_data.orchestration.landing.service import StaticSourceLandingService
from flashflood_data.orchestration.landing.sources import SourceLandingError
from flashflood_data.storage.object_store import ObjectPublisher


class MemoryObjectStore:
    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.data

    def size(self, key: str) -> int:
        return len(self.data[key])

    def sha256(self, key: str) -> str:
        return sha256(self.data[key]).hexdigest()

    def upload(self, local_path: Path, key: str, metadata: Mapping[str, str]) -> None:
        self.data[key] = local_path.read_bytes()

    def copy(self, source_key: str, destination_key: str) -> None:
        self.data[destination_key] = self.data[source_key]

    def delete(self, key: str) -> None:
        self.data.pop(key, None)


class FakeInventory:
    def __init__(self) -> None:
        self.object_ids: set[str] = set()

    def register_many(self, rows):
        reused = sum(row.object_id in self.object_ids for row in rows)
        self.object_ids.update(row.object_id for row in rows)
        return RegisteredBatch(
            source_id=rows[0].source_id,
            run_id=rows[0].ingest_run_id,
            object_ids=tuple(row.object_id for row in rows),
            snapshot_id=42,
            reused=reused,
        )


def _prepared(staging: Path, source_id: str, run_id: str) -> PreparedObject:
    path = staging / run_id / source_id / "source.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"verified-{source_id}".encode())
    return PreparedObject(
        source_id=source_id,
        source_version="1",
        asset_id=f"asset-{source_id}",
        path=path,
        filename="source.bin",
        media_type="application/octet-stream",
        source_uri="https://user:password@example.invalid/source?token=secret",
        license_id="fixture-license",
        retrieved_at=datetime(2026, 9, 16, tzinfo=UTC),
        selection={"basin_level": 12},
    )


def _service(tmp_path: Path, *, failing_source: str | None = None) -> StaticSourceLandingService:
    sources = tuple(
        LandingSourcePolicy(source_id=source_id, mode="individual")
        for source_id in ("hydrobasins_v1c", "basinatlas_v10")
    )
    staging = tmp_path / "staging"

    def prepare(source_id: str, run_id: str):
        if source_id == failing_source:
            error = SourceLandingError("password=secret")
            error.code = "fixture_failure"
            raise error
        return (_prepared(staging, source_id, run_id),)

    store = MemoryObjectStore()
    return StaticSourceLandingService(
        config=StaticLandingConfig(basin_level=12, sources=sources),
        publisher=ObjectPublisher(store, "raw"),
        inventory=FakeInventory(),
        staging_root=staging,
        source_preparer=prepare,
    )


def test_service_publishes_manifest_before_registering_and_cleans_after_commit(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)

    batch = service.publish_source("hydrobasins_v1c", run_id="run-1")

    assert service.store.exists(batch.objects[0].object_key)
    assert service.store.exists(batch.objects[0].manifest_key)
    assert (tmp_path / "staging" / "run-1" / "hydrobasins_v1c").exists()
    registered = service.register_batch(batch)
    service.cleanup_batch(registered)
    assert registered.snapshot_id == 42
    assert not (tmp_path / "staging" / "run-1" / "hydrobasins_v1c").exists()


def test_recovery_reuses_objects_manifests_and_inventory_rows(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.publish_source("hydrobasins_v1c", run_id="run-1")
    first_registered = service.register_batch(first)

    second = service.publish_source("hydrobasins_v1c", run_id="run-1")
    second_registered = service.register_batch(second)

    assert all(item.reused for item in second.objects)
    assert first_registered.object_ids == second_registered.object_ids
    assert second_registered.reused == 1


def test_run_continues_independent_source_and_reports_sanitized_partial_failure(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path, failing_source="basinatlas_v10")

    summary = service.run(["hydrobasins_v1c", "basinatlas_v10"], run_id="run-1")

    assert summary.status == "partial_failure"
    assert summary.completed_sources == ("hydrobasins_v1c",)
    assert summary.failed_sources == ("basinatlas_v10",)
    assert summary.errors == {"basinatlas_v10": "fixture_failure"}
    assert "secret" not in summary.model_dump_json()
