"""Immutable Raw publication for dynamic weather provider objects."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol

from flashflood_data.orchestration.landing.models import (
    LandingManifest,
    SourceObjectRow,
)
from flashflood_data.orchestration.weather.models import (
    FetchedWeatherObject,
    PublishedWeatherObject,
)
from flashflood_data.storage.object_store import ObjectConflict, ObjectPublisher


class _Inventory(Protocol):
    def register_many(self, rows): ...


class _Meta(Protocol):
    def record_attempt(self, **values): ...


def _checksum(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_text(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class WeatherLandingService:
    """Publish one fetched response and register its immutable Raw identity."""

    def __init__(
        self,
        *,
        publisher: ObjectPublisher,
        inventory: _Inventory,
        meta: _Meta,
        license_id: str,
    ) -> None:
        self.publisher = publisher
        self.inventory = inventory
        self.meta = meta
        self.license_id = license_id

    @staticmethod
    def _object_id(fetched: FetchedWeatherObject, checksum: str) -> str:
        identity = (
            f"{fetched.planned.source_id}\0{fetched.planned.source_version}\0"
            f"{fetched.planned.asset_id}\0{checksum}"
        )
        return sha256(identity.encode("utf-8")).hexdigest()

    @staticmethod
    def _prefix(fetched: FetchedWeatherObject, checksum: str) -> str:
        when = fetched.planned.window.start.astimezone(UTC)
        return "/".join(
            (
                "weather",
                fetched.planned.source_id,
                fetched.planned.product,
                when.strftime("%Y"),
                when.strftime("%m"),
                when.strftime("%d"),
                fetched.planned.asset_id,
                checksum,
            )
        )

    def publish_and_register(
        self, fetched: FetchedWeatherObject, *, run_id: str, attempt_no: int = 1
    ) -> PublishedWeatherObject:
        """Commit payload, manifest, then Iceberg inventory; delete staging last."""
        started = datetime.now(UTC)
        planned = fetched.planned
        try:
            checksum = _checksum(fetched.path)
            prefix = self._prefix(fetched, checksum)
            payload = self.publisher.publish_file(
                fetched.path,
                final_key=f"{prefix}/{fetched.filename}",
                run_id=run_id,
                media_type=fetched.media_type,
            )
            object_id = self._object_id(fetched, checksum)
            selection = {
                "stream_id": planned.stream_id,
                "window_start": _utc_text(planned.window.start),
                "window_end": _utc_text(planned.window.end),
                "variables": list(planned.variables),
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
                media_type=fetched.media_type,
                selection=selection,
                source_uri=fetched.source_uri,
                request_fingerprint=planned.request_fingerprint,
                license_id=self.license_id,
                retrieval_run_id=run_id,
                retrieved_at=fetched.retrieved_at,
                source_valid_time=_utc_text(planned.window.start),
                object_uri=payload.object_uri,
                size_bytes=payload.size_bytes,
                checksum=payload.checksum,
                provider_metadata=dict(fetched.provider_metadata),
            )
            manifest_key = f"{prefix}/{fetched.filename}.manifest.json"
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
                manifest_path = fetched.path.parent / f"manifest-{fetched.filename}.json"
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
                media_type=fetched.media_type,
                size_bytes=payload.size_bytes,
                checksum=payload.checksum,
                source_uri=fetched.source_uri,
                provider_issued_at=_utc_text(fetched.provider_issued_at),
                model_run_time=_utc_text(planned.model_run_time),
                valid_time=_utc_text(planned.window.start),
                available_at=_utc_text(fetched.available_at),
                retrieved_at=manifest.retrieved_at,
                first_seen_at=manifest.retrieved_at,
                ingest_run_id=run_id,
                selection_json=json.dumps(selection, sort_keys=True, separators=(",", ":")),
                provider_metadata_json=json.dumps(
                    dict(fetched.provider_metadata), sort_keys=True, separators=(",", ":")
                ),
            )
            batch = self.inventory.register_many([row])
            self.meta.record_attempt(
                ingest_run_id=run_id,
                source_id=planned.source_id,
                asset_id=planned.asset_id,
                attempt_no=attempt_no,
                request_fingerprint=planned.request_fingerprint,
                status="succeeded",
                started_at=started,
                ended_at=datetime.now(UTC),
            )
            fetched.path.unlink(missing_ok=True)
            return PublishedWeatherObject(
                source_id=planned.source_id,
                stream_id=planned.stream_id,
                product=planned.product,
                object_id=object_id,
                asset_id=planned.asset_id,
                window=planned.window,
                object_uri=payload.object_uri,
                snapshot_id=batch.snapshot_id,
                reused=payload.reused,
            )
        except Exception as error:
            self.meta.record_attempt(
                ingest_run_id=run_id,
                source_id=planned.source_id,
                asset_id=planned.asset_id,
                attempt_no=attempt_no,
                request_fingerprint=planned.request_fingerprint,
                status="failed",
                started_at=started,
                ended_at=datetime.now(UTC),
                error_code=type(error).__name__,
            )
            raise
