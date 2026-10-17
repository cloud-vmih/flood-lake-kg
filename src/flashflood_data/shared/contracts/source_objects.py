"""Source-object contracts shared by Landing, Bronze, weather, and storage."""

from datetime import datetime
from typing import Literal

from pydantic import Field

from flashflood_data.catalog.models import ImmutableModel


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


class RawCleanupCandidate(ImmutableModel):
    """A downloaded local raw file backed by a committed MinIO object."""

    object_id: str
    asset_id: str
    storage_path: str
    object_key: str
    size_bytes: int = Field(ge=0)
    checksum: str


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


class RegisteredBatch(ImmutableModel):
    """Published batch after a committed Iceberg inventory snapshot."""

    source_id: str
    run_id: str
    object_ids: tuple[str, ...] = ()
    snapshot_id: int | None = None
    reused: int = Field(default=0, ge=0)
    cleanup_paths: tuple[str, ...] = ()
    raw_cleanup: tuple[RawCleanupCandidate, ...] = ()
