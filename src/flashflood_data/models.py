"""Immutable value models shared by pipeline stages."""

from datetime import datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class AssetStatus(StrEnum):
    """Lifecycle state of a catalogued asset."""

    DISCOVERED = "discovered"
    FETCHING = "fetching"
    FETCHED = "fetched"
    VALIDATED = "validated"
    HARMONIZED = "harmonized"
    DERIVED = "derived"
    FAILED = "failed"
    STALE = "stale"
    QUARANTINED = "quarantined"


class AssetKind(StrEnum):
    """The pipeline layer represented by an asset."""

    RAW = "raw"
    HARMONIZED = "harmonized"
    DERIVED = "derived"
    QA = "qa"


class ImmutableModel(BaseModel):
    """Base class for records that are replaced rather than mutated."""

    model_config = ConfigDict(frozen=True)


class AssetRecord(ImmutableModel):
    """Auditable metadata for one immutable file asset."""

    asset_id: str
    source_id: str
    source_version: str
    kind: AssetKind
    source_uri: str
    storage_path: str
    media_type: str
    size_bytes: int = Field(ge=0)
    checksum_algorithm: str = "sha256"
    checksum: str
    retrieved_at: datetime
    source_valid_time: str | None = None
    license_id: str
    bbox_wgs84_json: str | None = None
    crs: str | None = None
    resolution_json: str | None = None
    pipeline_run_id: str
    status: AssetStatus
    dependency_fingerprint: str | None = None
    duplicate_of_asset_id: str | None = None
    metadata_json: str = "{}"
    error_code: str | None = None
    error_message: str | None = None


class RunRecord(ImmutableModel):
    """Auditable record of an independently runnable pipeline command."""

    run_id: str
    command: str
    started_at: datetime
    ended_at: datetime | None = None
    status: str = "running"
    config_fingerprint: str


class RemoteAsset(ImmutableModel):
    """Source-declared remote payload metadata, excluding credentials."""

    asset_id: str
    source_id: str
    source_version: str
    uri: str
    target_relative_path: Path
    media_type: str
    license_id: str
    expected_size: int | None = None
    expected_checksum: str | None = None
    source_valid_time: str | None = None
    request_method: str = "GET"
    request_form: dict[str, str] = Field(default_factory=dict)


class SourceSpec(ImmutableModel):
    """Static configuration for one source adapter."""

    source_id: str
    adapter: str
    version: str
    license_id: str
    enabled: bool = True
    settings: dict[str, object] = Field(default_factory=dict)


class SourceFile(ImmutableModel):
    """Source registry document."""

    sources: list[SourceSpec]


class ValidationResult(ImmutableModel):
    """Machine-readable outcome of a validation stage."""

    passed: bool
    checks: dict[str, bool]
    metrics: dict[str, float | int | str] = Field(default_factory=dict)
    messages: list[str] = Field(default_factory=list)
