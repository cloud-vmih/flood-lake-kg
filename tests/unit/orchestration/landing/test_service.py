import json
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from flashflood_data.catalog import AssetCatalog
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    SourceSpec,
    ValidationResult,
)
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.landing.config import (
    LandingSourcePolicy,
    StaticLandingConfig,
)
from flashflood_data.orchestration.landing.models import PreparedObject, RegisteredBatch
from flashflood_data.orchestration.landing.service import StaticSourceLandingService
from flashflood_data.orchestration.landing.sources import SourceLandingError
from flashflood_data.storage.http import BudgetRejected
from flashflood_data.storage.object_store import ObjectConflict, ObjectPublisher


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

    def read(self, key: str) -> bytes:
        return self.data[key]

    def download(self, key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(self.data[key])


class FakeInventory:
    def __init__(self) -> None:
        self.object_ids: set[str] = set()
        self.rows = {}

    def register_many(self, rows):
        reused = sum(row.object_id in self.object_ids for row in rows)
        self.object_ids.update(row.object_id for row in rows)
        self.rows.update({row.object_id: row for row in rows})
        return RegisteredBatch(
            source_id=rows[0].source_id,
            run_id=rows[0].ingest_run_id,
            object_ids=tuple(row.object_id for row in rows),
            snapshot_id=42,
            reused=reused,
        )

    def source_locations(self, source_id: str):
        return {
            (row.asset_id, row.source_version, row.checksum): (row.object_uri, row.size_bytes)
            for row in self.rows.values() if row.source_id == source_id
        }


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


def test_service_cleans_failed_source_staging_before_the_next_source(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    source_root = tmp_path / "staging" / "run-1" / "hydrobasins_v1c"
    source_root.mkdir(parents=True)
    (source_root / "partial.bin").write_bytes(b"partial")

    service.cleanup_failed_source("hydrobasins_v1c", "run-1")

    assert not source_root.exists()


def test_recovery_reuses_objects_manifests_and_inventory_rows(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.publish_source("hydrobasins_v1c", run_id="run-1")
    first_registered = service.register_batch(first)

    second = service.publish_source("hydrobasins_v1c", run_id="run-1")
    second_registered = service.register_batch(second)

    assert all(item.reused for item in second.objects)
    assert first_registered.object_ids == second_registered.object_ids
    assert second_registered.reused == 1


def test_later_run_reuses_existing_object_manifest(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.publish_source("hydrobasins_v1c", run_id="run-1")
    service.register_batch(first)

    second = service.publish_source("hydrobasins_v1c", run_id="run-2")

    assert all(item.reused for item in second.objects)
    assert second.objects[0].manifest_uri == first.objects[0].manifest_uri


def test_later_run_rejects_corrupt_existing_manifest(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.publish_source("hydrobasins_v1c", run_id="run-1")
    service.register_batch(first)
    service.store.data[first.objects[0].manifest_key] = b"{}"

    with pytest.raises(ObjectConflict, match="immutable manifest conflict"):
        service.publish_source("hydrobasins_v1c", run_id="run-2")


def test_later_run_rejects_manifest_with_different_checksum_contract(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)
    first = service.publish_source("hydrobasins_v1c", run_id="run-1")
    manifest_key = first.objects[0].manifest_key
    manifest = json.loads(service.store.data[manifest_key])
    manifest["checksum_algorithm"] = "md5"
    service.store.data[manifest_key] = json.dumps(manifest).encode()

    with pytest.raises(ObjectConflict, match="immutable manifest conflict"):
        service.publish_source("hydrobasins_v1c", run_id="run-2")


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


def test_storage_budget_rejection_has_an_actionable_error_code() -> None:
    error = BudgetRejected("minimum_free_space")

    assert StaticSourceLandingService._error_code(error) == "storage_budget_rejected"


def test_committed_remote_raw_is_removed_but_existing_source_is_preserved(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    catalog = AssetCatalog(paths)
    sources = {
        "gadm_vnm_4_1": "gadm_admin",
        "hydrobasins_v1c": "existing",
    }
    policies = tuple(LandingSourcePolicy(source_id=name, mode="individual") for name in sources)
    files = {
        "gadm_vnm_4_1": paths.raw / "gadm.zip",
        "hydrobasins_v1c": paths.raw / "hydrobasins.zip",
    }
    for source_id, path in files.items():
        path.write_bytes(source_id.encode())
        catalog.upsert(AssetRecord(
            asset_id=f"asset-{source_id}", source_id=source_id, source_version="1",
            kind=AssetKind.RAW, source_uri="https://example.invalid/source",
            storage_path=str(path), media_type="application/zip",
            size_bytes=path.stat().st_size, checksum=sha256(path.read_bytes()).hexdigest(),
            retrieved_at=datetime(2026, 9, 16, tzinfo=UTC), license_id="fixture",
            pipeline_run_id="run-1", status=AssetStatus.VALIDATED,
        ))

    def prepare(source_id: str, run_id: str):
        path = files[source_id]
        return (PreparedObject(
            source_id=source_id, source_version="1", asset_id=f"asset-{source_id}",
            path=path, filename=path.name, media_type="application/zip",
            source_uri="https://example.invalid/source", license_id="fixture",
            retrieved_at=datetime(2026, 9, 16, tzinfo=UTC),
        ),)

    store = MemoryObjectStore()
    service = StaticSourceLandingService(
        config=StaticLandingConfig(basin_level=12, sources=policies),
        publisher=ObjectPublisher(store, "raw"), inventory=FakeInventory(),
        staging_root=tmp_path / "staging", source_preparer=prepare,
        source_specs={name: SourceSpec(source_id=name, adapter=adapter, version="1", license_id="fixture")
                      for name, adapter in sources.items()},
        paths=paths, catalog=catalog,
    )

    remote_batch = service.publish_source("gadm_vnm_4_1", "run-1")
    local_batch = service.publish_source("hydrobasins_v1c", "run-1")
    assert all(path.exists() for path in files.values())

    service.cleanup_batch(service.register_batch(remote_batch))
    service.cleanup_batch(service.register_batch(local_batch))

    assert not files["gadm_vnm_4_1"].exists()
    assert files["hydrobasins_v1c"].exists()


def test_later_run_restores_committed_remote_raw_from_minio_without_provider_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    catalog = AssetCatalog(paths)
    raw = paths.raw / "gadm.zip"
    raw.write_bytes(b"raw-from-provider")
    checksum = sha256(raw.read_bytes()).hexdigest()
    source_id = "gadm_vnm_4_1"
    catalog.upsert(AssetRecord(
        asset_id="gadm-asset", source_id=source_id, source_version="1", kind=AssetKind.RAW,
        source_uri="https://example.invalid/gadm.zip", storage_path=str(raw),
        media_type="application/zip", size_bytes=raw.stat().st_size, checksum=checksum,
        retrieved_at=datetime(2026, 9, 16, tzinfo=UTC), license_id="fixture",
        pipeline_run_id="run-1", status=AssetStatus.VALIDATED,
    ))
    remote = RemoteAsset(
        asset_id="gadm-asset", source_id=source_id, source_version="1",
        uri="https://example.invalid/gadm.zip", target_relative_path=Path("raw/gadm.zip"),
        media_type="application/zip", license_id="fixture", expected_size=raw.stat().st_size,
        expected_checksum=checksum,
    )

    class Adapter:
        def resolve(self, context, available):
            return [remote]

        def validate_raw(self, path):
            return ValidationResult(passed=True, checks={"valid": True})

    class Fetcher:
        def fetch(self, remote, run_id):
            raise AssertionError("provider must not be fetched again")

    monkeypatch.setattr("flashflood_data.orchestration.landing.sources.build_adapter", lambda spec: Adapter())
    monkeypatch.setattr("flashflood_data.orchestration.landing.service.inventory_existing", lambda context: [])
    store = MemoryObjectStore()
    inventory = FakeInventory()

    def service() -> StaticSourceLandingService:
        return StaticSourceLandingService(
            config=StaticLandingConfig(basin_level=12, sources=(LandingSourcePolicy(source_id=source_id, mode="individual"),)),
            publisher=ObjectPublisher(store, "raw"), inventory=inventory,
            staging_root=tmp_path / "staging",
            source_specs={source_id: SourceSpec(source_id=source_id, adapter="gadm_admin", version="1", license_id="fixture")},
            paths=paths, catalog=catalog, fetcher=Fetcher(),
            study_area=StudyAreaConfig(), environment=EnvironmentSettings(_env_file=None),
        )

    first_service = service()
    first = first_service.publish_source(source_id, "run-1")
    first_service.cleanup_batch(first_service.register_batch(first))
    assert not raw.exists()
    catalog.upsert(catalog.get("gadm-asset").model_copy(update={
        "storage_path": "/old-host/project/dataset/raw/gadm.zip",
    }))

    second_service = service()
    second = second_service.publish_source(source_id, "run-2")

    assert raw.read_bytes() == b"raw-from-provider"
    assert second.objects[0].reused
    second_service.cleanup_batch(second_service.register_batch(second))
    assert not raw.exists()


def test_cleanup_preserves_local_raw_when_minio_copy_is_corrupt(tmp_path: Path) -> None:
    paths = ProjectPaths.discover(tmp_path)
    paths.ensure_output_dirs()
    catalog = AssetCatalog(paths)
    raw = paths.raw / "gadm.zip"
    raw.write_bytes(b"correct")
    catalog.upsert(AssetRecord(
        asset_id="gadm-asset", source_id="gadm_vnm_4_1", source_version="1",
        kind=AssetKind.RAW, source_uri="https://example.invalid/gadm.zip",
        storage_path=str(raw), media_type="application/zip", size_bytes=raw.stat().st_size,
        checksum=sha256(raw.read_bytes()).hexdigest(),
        retrieved_at=datetime(2026, 9, 16, tzinfo=UTC), license_id="fixture",
        pipeline_run_id="run-1", status=AssetStatus.VALIDATED,
    ))
    prepared = PreparedObject(
        source_id="gadm_vnm_4_1", source_version="1", asset_id="gadm-asset",
        path=raw, filename=raw.name, media_type="application/zip",
        source_uri="https://example.invalid/gadm.zip", license_id="fixture",
        retrieved_at=datetime(2026, 9, 16, tzinfo=UTC),
    )
    store = MemoryObjectStore()
    service = StaticSourceLandingService(
        config=StaticLandingConfig(basin_level=12, sources=(LandingSourcePolicy(source_id="gadm_vnm_4_1", mode="individual"),)),
        publisher=ObjectPublisher(store, "raw"), inventory=FakeInventory(),
        staging_root=tmp_path / "staging", source_preparer=lambda source_id, run_id: (prepared,),
        source_specs={"gadm_vnm_4_1": SourceSpec(source_id="gadm_vnm_4_1", adapter="gadm_admin", version="1", license_id="fixture")},
        paths=paths, catalog=catalog,
    )
    batch = service.publish_source("gadm_vnm_4_1", "run-1")
    registered = service.register_batch(batch)
    store.data[batch.objects[0].object_key] = b"corrupt"

    with pytest.raises(ObjectConflict, match="verified MinIO copy"):
        service.cleanup_batch(registered)

    assert raw.read_bytes() == b"correct"
