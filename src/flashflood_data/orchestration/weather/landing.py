"""Immutable Raw publication for dynamic weather provider objects."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from flashflood_data.orchestration.landing.models import (
    LandingManifest,
    SourceObjectRow,
)
from flashflood_data.orchestration.weather.models import (
    PublishedWeatherObject,
    RetentionClass,
    ScopedWeatherObject,
)
from flashflood_data.storage.object_store import ObjectConflict, ObjectPublisher


class _Inventory(Protocol):
    def register_many(self, rows): ...


class _Meta(Protocol):
    def record_attempt(self, **values): ...

    def record_attempts(self, rows): ...


class _Lifecycle(Protocol):
    def register(
        self,
        object_id: str,
        retention_class: RetentionClass,
        published_at: datetime,
        run_id: str,
    ) -> int: ...

    def register_many(self, entries): ...


def _checksum(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_text(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class _PreparedPublication:
    scoped: ScopedWeatherObject
    row: SourceObjectRow
    started_at: datetime
    published_at: datetime
    reused: bool


class WeatherLandingService:
    """Publish one fetched response and register its immutable Raw identity."""

    def __init__(
        self,
        *,
        publisher: ObjectPublisher,
        inventory: _Inventory,
        meta: _Meta,
        lifecycle: _Lifecycle,
        retention_class: RetentionClass,
        license_id: str,
    ) -> None:
        self.publisher = publisher
        self.inventory = inventory
        self.meta = meta
        self.lifecycle = lifecycle
        self.retention_class = retention_class
        self.license_id = license_id

    @staticmethod
    def _object_id(scoped: ScopedWeatherObject, checksum: str) -> str:
        identity = (
            f"{scoped.planned.source_id}\0{scoped.planned.source_version}\0"
            f"{scoped.planned.asset_id}\0{checksum}"
        )
        return sha256(identity.encode("utf-8")).hexdigest()

    @staticmethod
    def _prefix(scoped: ScopedWeatherObject, checksum: str) -> str:
        when = scoped.planned.window.start.astimezone(UTC)
        return "/".join(
            (
                "weather",
                scoped.planned.source_id,
                scoped.planned.product,
                when.strftime("%Y"),
                when.strftime("%m"),
                when.strftime("%d"),
                scoped.planned.asset_id,
                checksum,
            )
        )

    def _prepare_publication(
        self, scoped: ScopedWeatherObject, *, run_id: str
    ) -> _PreparedPublication:
        """Publish immutable bytes and build the row for a later batch commit."""
        started = datetime.now(UTC)
        planned = scoped.planned
        if planned.spatial_scope_id != scoped.spatial_scope_id:
            raise ValueError("planned and scoped weather AOI identities do not match")
        checksum = _checksum(scoped.path)
        if checksum != scoped.scoped_payload_checksum:
            raise ValueError("scoped weather payload checksum changed before publication")
        if scoped.path.stat().st_size != scoped.scoped_payload_size_bytes:
            raise ValueError("scoped weather payload size changed before publication")
        prefix = self._prefix(scoped, checksum)
        payload = self.publisher.publish_file(
            scoped.path,
            final_key=f"{prefix}/{scoped.filename}",
            run_id=run_id,
            media_type=scoped.media_type,
        )
        object_id = self._object_id(scoped, checksum)
        selection = {
            "stream_id": planned.stream_id,
            "window_start": _utc_text(planned.window.start),
            "window_end": _utc_text(planned.window.end),
            "variables": list(planned.variables),
            "spatial_scope_id": scoped.spatial_scope_id,
            "source_grid_version": scoped.source_grid_version,
            "cell_count": len(scoped.cell_indices),
            "source_cycle_id": planned.source_cycle_id,
            "source_revision": (
                planned.source_revision
                if planned.source_revision > 0
                else int(checksum[:8], 16) & 0x7FFFFFFF
            ),
            "options": dict(planned.options),
        }
        expected_manifest = LandingManifest(
            object_id=object_id,
            asset_id=planned.asset_id,
            source_id=planned.source_id,
            source_version=planned.source_version,
            source_type="dynamic",
            product=planned.product,
            media_type=scoped.media_type,
            selection=selection,
            source_uri=scoped.source_uri,
            request_fingerprint=planned.request_fingerprint,
            license_id=self.license_id,
            retrieval_run_id=run_id,
            retrieved_at=scoped.retrieved_at,
            source_valid_time=_utc_text(planned.window.start),
            object_uri=payload.object_uri,
            size_bytes=payload.size_bytes,
            checksum=payload.checksum,
            source_archive_checksum=scoped.provider_payload_checksum,
            source_archive_size_bytes=scoped.provider_payload_size_bytes,
            provider_metadata={
                **dict(scoped.provider_metadata),
                "provider_payload_checksum": scoped.provider_payload_checksum,
                "provider_payload_size_bytes": scoped.provider_payload_size_bytes,
            },
        )
        manifest_key = f"{prefix}/{scoped.filename}.manifest.json"
        existing = self.publisher.find_existing(manifest_key, "application/json")
        if existing is not None:
            stored = LandingManifest.model_validate_json(
                self.publisher.read_existing(manifest_key)
            )
            for field in (
                "object_id", "asset_id", "source_id", "source_version", "product",
                "request_fingerprint", "object_uri", "size_bytes", "checksum",
            ):
                if getattr(stored, field) != getattr(expected_manifest, field):
                    raise ObjectConflict(f"immutable weather manifest conflict: {manifest_key}")
            manifest = stored
            published_manifest = existing
        else:
            manifest = expected_manifest
            manifest_path = scoped.path.parent / f"manifest-{scoped.filename}.json"
            manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")
            try:
                published_manifest = self.publisher.publish_file(
                    manifest_path,
                    final_key=manifest_key,
                    run_id=run_id,
                    media_type="application/json",
                )
            finally:
                manifest_path.unlink(missing_ok=True)
        row = SourceObjectRow(
            object_id=object_id,
            asset_id=planned.asset_id,
            source_id=planned.source_id,
            source_version=planned.source_version,
            source_type="dynamic",
            product=planned.product,
            object_uri=payload.object_uri,
            manifest_uri=published_manifest.object_uri,
            media_type=scoped.media_type,
            size_bytes=payload.size_bytes,
            checksum=payload.checksum,
            source_uri=scoped.source_uri,
            provider_issued_at=_utc_text(scoped.provider_issued_at),
            model_run_time=_utc_text(planned.model_run_time),
            valid_time=_utc_text(planned.window.start),
            available_at=_utc_text(scoped.available_at),
            retrieved_at=manifest.retrieved_at,
            first_seen_at=manifest.retrieved_at,
            ingest_run_id=run_id,
            selection_json=json.dumps(selection, sort_keys=True, separators=(",", ":")),
            provider_metadata_json=json.dumps(
                dict(manifest.provider_metadata), sort_keys=True, separators=(",", ":")
            ),
        )
        return _PreparedPublication(
            scoped=scoped,
            row=row,
            started_at=started,
            published_at=manifest.retrieved_at,
            reused=payload.reused,
        )

    def publish_many(
        self,
        scoped_objects: list[ScopedWeatherObject],
        *,
        run_id: str,
        attempt_no: int = 1,
    ) -> tuple[PublishedWeatherObject, ...]:
        """Publish one mapped batch and commit its shared Iceberg rows once."""
        if not scoped_objects:
            return ()
        prepared: list[_PreparedPublication] = []
        current = scoped_objects[0]
        started = datetime.now(UTC)
        try:
            for current in scoped_objects:
                started = datetime.now(UTC)
                prepared.append(self._prepare_publication(current, run_id=run_id))
            batch = self.inventory.register_many([item.row for item in prepared])
            self.lifecycle.register_many(
                [
                    (item.row.object_id, self.retention_class, item.published_at, run_id)
                    for item in prepared
                ]
            )
            ended_at = datetime.now(UTC)
            self.meta.record_attempts(
                [
                    {
                        "ingest_run_id": run_id,
                        "source_id": item.scoped.planned.source_id,
                        "asset_id": item.scoped.planned.asset_id,
                        "attempt_no": attempt_no,
                        "request_fingerprint": item.scoped.planned.request_fingerprint,
                        "status": "succeeded",
                        "started_at": item.started_at,
                        "ended_at": ended_at,
                        "http_status": None,
                        "error_code": None,
                    }
                    for item in prepared
                ]
            )
            results = tuple(
                PublishedWeatherObject(
                    source_id=item.scoped.planned.source_id,
                    stream_id=item.scoped.planned.stream_id,
                    product=item.scoped.planned.product,
                    object_id=item.row.object_id,
                    asset_id=item.scoped.planned.asset_id,
                    window=item.scoped.planned.window,
                    object_uri=item.row.object_uri,
                    snapshot_id=batch.snapshot_id,
                    reused=item.reused,
                )
                for item in prepared
            )
            return results
        except Exception as error:
            self.meta.record_attempt(
                ingest_run_id=run_id,
                source_id=current.planned.source_id,
                asset_id=current.planned.asset_id,
                attempt_no=attempt_no,
                request_fingerprint=current.planned.request_fingerprint,
                status="failed",
                started_at=started,
                ended_at=datetime.now(UTC),
                error_code=type(error).__name__,
            )
            raise

    def publish_and_register(
        self, scoped: ScopedWeatherObject, *, run_id: str, attempt_no: int = 1
    ) -> PublishedWeatherObject:
        """Publish one object through the same batched commit path used by Airflow."""
        return self.publish_many(
            [scoped], run_id=run_id, attempt_no=attempt_no
        )[0]
