"""Recoverable application service for static raw-source landing."""

import json
import shutil
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from flashflood_data.catalog import AssetCatalog
from flashflood_data.catalog.models import SourceSpec
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.landing.config import StaticLandingConfig
from flashflood_data.orchestration.landing.models import (
    BundleMember,
    LandingManifest,
    LandingRunSummary,
    PreparedObject,
    PublishedBatch,
    PublishedObject,
    RegisteredBatch,
    SourceObjectRow,
)
from flashflood_data.orchestration.landing.sources import (
    SourceLandingError,
    acquire_validated_assets,
    prepare_source_objects,
)
from flashflood_data.static.sources.base import SourceContext
from flashflood_data.static.sources.existing import inventory_existing
from flashflood_data.storage.http import BudgetRejected
from flashflood_data.storage.http.fetcher import HttpFetcher
from flashflood_data.storage.http.redaction import redact
from flashflood_data.storage.iceberg import (
    IcebergCommitError,
    SourceObjectConflict,
    SourceObjectInventory,
)
from flashflood_data.storage.object_store import ObjectConflict, ObjectPublisher

SourcePreparer = Callable[[str, str], tuple[PreparedObject, ...]]
_MANIFEST_RUN_ANNOTATIONS = frozenset(
    {"retrieval_run_id", "source_uri", "provider_metadata"}
)
_MANIFEST_IDENTITY_FIELDS = tuple(
    field
    for field in LandingManifest.model_fields
    if field not in _MANIFEST_RUN_ANNOTATIONS
)


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _object_id(prepared: PreparedObject, checksum: str) -> str:
    identity = {
        "asset_id": prepared.asset_id,
        "checksum": checksum,
        "manifest_schema_version": 1,
        "selection": dict(prepared.selection),
        "source_id": prepared.source_id,
        "source_version": prepared.source_version,
    }
    return sha256(_canonical_json(identity).encode("utf-8")).hexdigest()


def _selection_segments(selection: Mapping[str, object]) -> tuple[str, ...]:
    segments = []
    for key, value in sorted(selection.items()):
        if not isinstance(value, (str, int, float, bool)) or isinstance(value, bytes):
            raise TypeError("published object selection values must be scalar")
        segments.append(f"{key}={str(value).lower() if isinstance(value, bool) else value}")
    return tuple(segments)


def _source_staging_root(
    config: StaticLandingConfig, staging_root: Path, source_id: str, run_id: str
) -> Path:
    config.source(source_id)
    root = Path(staging_root).resolve()
    run_root = (root / run_id).resolve()
    source_root = (run_root / source_id).resolve()
    if not run_root.is_relative_to(root) or not source_root.is_relative_to(run_root):
        raise ValueError("source staging path escapes staging root")
    return source_root


def cleanup_committed_staging(
    config: StaticLandingConfig, staging_root: Path, batch: RegisteredBatch
) -> None:
    """Remove run-scoped local staging after an Iceberg commit."""
    if batch.snapshot_id is None:
        raise ValueError("cannot clean staging without a committed snapshot")
    root = _source_staging_root(config, staging_root, batch.source_id, batch.run_id)
    for raw_path in batch.cleanup_paths:
        path = Path(raw_path).resolve()
        if not path.is_relative_to(root):
            raise ValueError("cleanup path escapes source run staging")
    if batch.cleanup_paths and root.exists():
        shutil.rmtree(root)


def cleanup_failed_staging(
    config: StaticLandingConfig, staging_root: Path, source_id: str, run_id: str
) -> None:
    """Remove run-scoped local staging after a source terminates with failure."""
    root = _source_staging_root(config, staging_root, source_id, run_id)
    if root.exists():
        shutil.rmtree(root)


def _sanitized_metadata(metadata: Mapping[str, object]) -> dict[str, object]:
    serialized = _canonical_json(dict(metadata))
    return json.loads(redact(serialized))


class StaticSourceLandingService:
    """Publish source bytes, manifests, and Iceberg inventory rows in recovery order."""

    def __init__(
        self,
        *,
        config: StaticLandingConfig,
        publisher: ObjectPublisher,
        inventory: SourceObjectInventory,
        staging_root: Path,
        source_preparer: SourcePreparer | None = None,
        source_specs: Mapping[str, SourceSpec] | None = None,
        paths: ProjectPaths | None = None,
        catalog: AssetCatalog | None = None,
        fetcher: HttpFetcher | None = None,
        study_area: StudyAreaConfig | None = None,
        environment: EnvironmentSettings | None = None,
    ) -> None:
        self.config = config
        self.publisher = publisher
        self.store = publisher.store
        self.inventory = inventory
        self.staging_root = Path(staging_root).resolve()
        self.source_specs = dict(source_specs or {})
        self.paths = paths
        self.catalog = catalog
        self.fetcher = fetcher
        self.study_area = study_area
        self.environment = environment
        self._custom_source_preparer = source_preparer
        self._contexts: dict[str, SourceContext] = {}
        self._prepared_runs: set[str] = set()

    def prepare_run(self, run_id: str) -> None:
        """Create run staging and inventory legacy local sources exactly once."""
        if run_id in self._prepared_runs:
            return
        self.staging_root.mkdir(parents=True, exist_ok=True)
        if self._custom_source_preparer is None:
            if None in (
                self.paths,
                self.catalog,
                self.fetcher,
                self.study_area,
                self.environment,
            ):
                raise ValueError("default landing preparation dependencies are incomplete")
            context = SourceContext(
                paths=self.paths,
                catalog=self.catalog,
                study_area=self.study_area,
                environment=self.environment,
                run_id=run_id,
            )
            inventory_existing(context)
            self._contexts[run_id] = context
        self._prepared_runs.add(run_id)

    def _prepare_source(self, source_id: str, run_id: str) -> tuple[PreparedObject, ...]:
        if self._custom_source_preparer is not None:
            return self._custom_source_preparer(source_id, run_id)
        try:
            spec = self.source_specs[source_id]
        except KeyError as exc:
            raise SourceLandingError(f"unknown source: {source_id}") from exc
        policy = self.config.source(source_id)
        context = self._contexts[run_id]
        records = acquire_validated_assets(policy, spec, context, self.fetcher)
        return prepare_source_objects(
            policy,
            records,
            staging_root=self.staging_root,
            run_id=run_id,
        )

    @staticmethod
    def _object_key(prepared: PreparedObject) -> str:
        parts = (
            "static",
            prepared.source_id,
            prepared.source_version,
            *_selection_segments(prepared.selection),
            prepared.asset_id,
            prepared.filename,
        )
        return "/".join(parts)

    def _reuse_manifest(
        self, manifest_key: str, expected: LandingManifest
    ) -> PublishedObject | None:
        published = self.publisher.find_existing(manifest_key, "application/json")
        if published is None:
            return None
        try:
            stored = LandingManifest.model_validate_json(
                self.publisher.read_existing(manifest_key)
            )
        except (TypeError, ValueError) as exc:
            raise ObjectConflict(f"immutable manifest conflict: {manifest_key}") from exc
        if any(
            getattr(stored, field) != getattr(expected, field)
            for field in _MANIFEST_IDENTITY_FIELDS
        ):
            raise ObjectConflict(f"immutable manifest conflict: {manifest_key}")
        return published

    def _publish_prepared(self, prepared: PreparedObject, run_id: str) -> tuple[PublishedObject, SourceObjectRow, Path]:
        published = self.publisher.publish_file(
            prepared.path,
            final_key=self._object_key(prepared),
            run_id=run_id,
            media_type=prepared.media_type,
        )
        object_id = _object_id(prepared, published.checksum)
        clean_members = tuple(
            BundleMember(
                name=member.name,
                size_bytes=member.size_bytes,
                checksum=member.checksum,
            )
            for member in prepared.members
        )
        provider_metadata = _sanitized_metadata(prepared.provider_metadata)
        manifest = LandingManifest(
            object_id=object_id,
            asset_id=prepared.asset_id,
            source_id=prepared.source_id,
            source_version=prepared.source_version,
            product=prepared.source_id,
            media_type=prepared.media_type,
            selection=dict(prepared.selection),
            source_uri=redact(prepared.source_uri),
            request_fingerprint=(
                str(provider_metadata["request_fingerprint"])
                if "request_fingerprint" in provider_metadata
                else None
            ),
            license_id=prepared.license_id,
            retrieval_run_id=run_id,
            retrieved_at=prepared.retrieved_at,
            source_valid_time=prepared.source_valid_time,
            object_uri=published.object_uri,
            size_bytes=published.size_bytes,
            checksum=published.checksum,
            source_archive_checksum=prepared.source_archive_checksum,
            source_archive_size_bytes=prepared.source_archive_size_bytes,
            members=clean_members,
            provider_metadata=provider_metadata,
        )
        local_manifest = (
            self.staging_root
            / run_id
            / prepared.source_id
            / "manifests"
            / f"{object_id}.json"
        )
        local_manifest.parent.mkdir(parents=True, exist_ok=True)
        local_manifest.write_text(_canonical_json(manifest.model_dump(mode="json")), encoding="utf-8")
        manifest_key = str(Path(self._object_key(prepared)).parent / "manifest.json")
        published_manifest = (
            self._reuse_manifest(manifest_key, manifest)
            if published.reused
            else None
        )
        if published_manifest is None:
            published_manifest = self.publisher.publish_file(
                local_manifest,
                final_key=manifest_key,
                run_id=run_id,
                media_type="application/json",
            )
        completed = published.model_copy(
            update={
                "manifest_key": published_manifest.object_key,
                "manifest_uri": published_manifest.object_uri,
                "reused": published.reused and published_manifest.reused,
            }
        )
        now = datetime.now(UTC)
        row = SourceObjectRow(
            object_id=object_id,
            asset_id=prepared.asset_id,
            source_id=prepared.source_id,
            source_version=prepared.source_version,
            product=prepared.source_id,
            basin_level=(
                int(prepared.selection["basin_level"])
                if "basin_level" in prepared.selection
                else None
            ),
            object_uri=published.object_uri,
            manifest_uri=published_manifest.object_uri,
            media_type=prepared.media_type,
            size_bytes=published.size_bytes,
            checksum=published.checksum,
            source_uri=redact(prepared.source_uri),
            valid_time=prepared.source_valid_time,
            retrieved_at=prepared.retrieved_at,
            first_seen_at=now,
            ingest_run_id=run_id,
            selection_json=_canonical_json(dict(prepared.selection)),
            provider_metadata_json=_canonical_json(provider_metadata),
        )
        return completed, row, local_manifest

    def publish_source(self, source_id: str, run_id: str) -> PublishedBatch:
        """Acquire, validate, package, and publish one independent source."""
        self.prepare_run(run_id)
        self.config.source(source_id)
        prepared_objects = self._prepare_source(source_id, run_id)
        published: list[PublishedObject] = []
        rows: list[SourceObjectRow] = []
        cleanup: set[str] = set()
        run_source_root = (self.staging_root / run_id / source_id).resolve()
        for prepared in prepared_objects:
            item, row, manifest_path = self._publish_prepared(prepared, run_id)
            published.append(item)
            rows.append(row)
            cleanup.add(str(manifest_path))
            prepared_path = prepared.path.resolve()
            if prepared_path.is_relative_to(run_source_root):
                cleanup.add(str(prepared_path))
        return PublishedBatch(
            source_id=source_id,
            run_id=run_id,
            objects=tuple(published),
            rows=tuple(rows),
            cleanup_paths=tuple(sorted(cleanup)),
        )

    def register_batch(self, batch: PublishedBatch) -> RegisteredBatch:
        """Register all published objects for a source in one Iceberg commit."""
        registered = self.inventory.register_many(batch.rows)
        return registered.model_copy(update={"cleanup_paths": batch.cleanup_paths})

    def cleanup_batch(self, batch: RegisteredBatch) -> None:
        """Remove only the committed source's run-scoped local staging directory."""
        cleanup_committed_staging(self.config, self.staging_root, batch)

    def cleanup_failed_source(self, source_id: str, run_id: str) -> None:
        """Remove run-scoped local staging after a source terminates with failure."""
        cleanup_failed_staging(self.config, self.staging_root, source_id, run_id)

    @staticmethod
    def _error_code(error: Exception) -> str:
        if isinstance(error, SourceLandingError):
            return error.code
        if isinstance(error, ObjectConflict):
            return "object_conflict"
        if isinstance(error, SourceObjectConflict):
            return "iceberg_identity_conflict"
        if isinstance(error, IcebergCommitError):
            return "iceberg_commit_failed"
        if isinstance(error, BudgetRejected):
            return "storage_budget_rejected"
        return "unexpected_source_failure"

    def run(
        self, source_ids: Sequence[str], run_id: str | None = None
    ) -> LandingRunSummary:
        """Run requested sources independently and return a sanitized overall status."""
        actual_run_id = run_id or f"manual-{uuid.uuid4().hex}"
        completed: list[str] = []
        reused: list[str] = []
        failed: list[str] = []
        errors: dict[str, str] = {}
        snapshots: dict[str, int] = {}
        for source_id in sorted(set(source_ids)):
            try:
                published = self.publish_source(source_id, actual_run_id)
                registered = self.register_batch(published)
                self.cleanup_batch(registered)
                completed.append(source_id)
                if published.objects and all(item.reused for item in published.objects):
                    reused.append(source_id)
                if registered.snapshot_id is not None:
                    snapshots[source_id] = registered.snapshot_id
            except Exception as error:  # noqa: BLE001 - source isolation requires a safe envelope
                failed.append(source_id)
                errors[source_id] = self._error_code(error)
        status = (
            "completed"
            if completed and not failed
            else "partial_failure"
            if completed
            else "failed"
        )
        return LandingRunSummary(
            run_id=actual_run_id,
            status=status,
            completed_sources=tuple(sorted(completed)),
            reused_sources=tuple(sorted(reused)),
            failed_sources=tuple(sorted(failed)),
            errors=dict(sorted(errors.items())),
            snapshots=dict(sorted(snapshots.items())),
        )
