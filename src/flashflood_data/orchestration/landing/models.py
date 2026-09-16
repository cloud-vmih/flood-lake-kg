"""Immutable, JSON-safe contracts for source landing tasks."""

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from flashflood_data.catalog.models import ImmutableModel


class BundleMember(ImmutableModel):
    """One unchanged member of a deterministic source bundle."""

    name: str
    size_bytes: int = Field(ge=0)
    checksum: str
    path: Path | None = None


class PreparedObject(ImmutableModel):
    """Validated local object ready for immutable publication."""

    source_id: str
    source_version: str
    asset_id: str
    path: Path
    filename: str
    media_type: str
    source_uri: str
    license_id: str
    retrieved_at: datetime
    source_valid_time: str | None = None
    selection: dict[str, object] = Field(default_factory=dict)
    members: tuple[BundleMember, ...] = ()
    source_archive_checksum: str | None = None
    source_archive_size_bytes: int | None = None
    provider_metadata: dict[str, object] = Field(default_factory=dict)


class PublishedObject(ImmutableModel):
    """A locally verified object published to immutable object storage."""

    object_key: str
    object_uri: str
    media_type: str
    size_bytes: int = Field(ge=0)
    checksum: str
    reused: bool = False
    manifest_key: str | None = None
    manifest_uri: str | None = None


class SourceObjectRow(ImmutableModel):
    """One authoritative row in the Iceberg source-object inventory."""

    object_id: str
    asset_id: str
    source_id: str
    source_version: str
    source_type: str = "static"
    product: str
    basin_level: int | None = None
    object_uri: str
    manifest_uri: str
    media_type: str
    size_bytes: int = Field(ge=0)
    checksum_algorithm: str = "sha256"
    checksum: str
    source_uri: str
    provider_issued_at: str | None = None
    model_run_time: str | None = None
    valid_time: str | None = None
    available_at: str | None = None
    retrieved_at: datetime
    first_seen_at: datetime
    ingest_run_id: str
    status: Literal["available"] = "available"
    selection_json: str = "{}"
    provider_metadata_json: str = "{}"


class LandingManifest(ImmutableModel):
    """Credential-free description of one verified raw object."""

    schema_version: int = 1
    object_id: str
    asset_id: str
    source_id: str
    source_version: str
    source_type: str = "static"
    product: str
    media_type: str
    selection: dict[str, object] = Field(default_factory=dict)
    source_uri: str
    request_fingerprint: str | None = None
    license_id: str
    retrieval_run_id: str
    retrieved_at: datetime
    source_valid_time: str | None = None
    object_uri: str
    size_bytes: int = Field(ge=0)
    checksum_algorithm: str = "sha256"
    checksum: str
    source_archive_checksum: str | None = None
    source_archive_size_bytes: int | None = None
    members: tuple[BundleMember, ...] = ()
    object_status: Literal["verified"] = "verified"
    provider_metadata: dict[str, object] = Field(default_factory=dict)


class PublishedBatch(ImmutableModel):
    """Small metadata-only result passed from publish to register."""

    source_id: str
    run_id: str
    objects: tuple[PublishedObject, ...] = ()
    rows: tuple[SourceObjectRow, ...] = ()
    cleanup_paths: tuple[str, ...] = ()


class RegisteredBatch(ImmutableModel):
    """Published batch after a committed Iceberg inventory snapshot."""

    source_id: str
    run_id: str
    object_ids: tuple[str, ...] = ()
    snapshot_id: int | None = None
    reused: int = Field(default=0, ge=0)
    cleanup_paths: tuple[str, ...] = ()


class LandingTaskEnvelope(ImmutableModel):
    """Success or sanitized failure passed between Airflow tasks."""

    status: Literal["success", "failure"]
    phase: Literal["published", "registered", "cleaned", "failed"]
    source_id: str
    batch: PublishedBatch | RegisteredBatch | None = None
    error_code: str | None = None

    @model_validator(mode="after")
    def payload_matches_status(self) -> "LandingTaskEnvelope":
        if self.status == "success" and self.batch is None:
            raise ValueError("successful envelope requires a batch")
        if self.status == "failure" and (self.batch is not None or not self.error_code):
            raise ValueError("failed envelope requires only an error code")
        return self

    @classmethod
    def succeeded(
        cls, phase: Literal["published", "registered", "cleaned"], batch: PublishedBatch | RegisteredBatch
    ) -> "LandingTaskEnvelope":
        return cls(status="success", phase=phase, source_id=batch.source_id, batch=batch)

    @classmethod
    def failed(cls, source_id: str, error_code: str) -> "LandingTaskEnvelope":
        return cls(
            status="failure",
            phase="failed",
            source_id=source_id,
            error_code=error_code,
        )


class LandingRunSummary(ImmutableModel):
    """Sanitized final outcome for CLI and Airflow."""

    run_id: str
    status: Literal["completed", "partial_failure", "failed"]
    completed_sources: tuple[str, ...] = ()
    reused_sources: tuple[str, ...] = ()
    failed_sources: tuple[str, ...] = ()
    errors: dict[str, str] = Field(default_factory=dict)
    snapshots: dict[str, int] = Field(default_factory=dict)
