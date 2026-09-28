"""Immutable contracts shared by dynamic weather providers and Airflow tasks."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator

from flashflood_data.catalog.models import ImmutableModel

RetentionClass = Literal["durable", "transient_7d"]
StorageStatus = Literal[
    "available", "eligible_for_expiry", "expired", "delete_failed"
]


class WeatherWindow(ImmutableModel):
    """Half-open UTC interval represented by one expected provider object."""

    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_range(self) -> "WeatherWindow":
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("weather windows must be timezone-aware")
        if self.start.utcoffset() != UTC.utcoffset(self.start) or self.end.utcoffset() != UTC.utcoffset(self.end):
            raise ValueError("weather windows must use UTC")
        if self.end <= self.start:
            raise ValueError("weather window end must be after start")
        return self


class WeatherStreamConfig(ImmutableModel):
    """One independently cursor-tracked product stream within a provider."""

    stream_id: str
    product: str
    start_at: datetime
    step_minutes: int = Field(gt=0)
    chunk_minutes: int = Field(gt=0)
    chunking: Literal["fixed", "calendar_month"] = "fixed"
    availability_lag_minutes: int = Field(ge=0)
    overlap_steps: int = Field(default=0, ge=0)
    variables: tuple[str, ...]
    media_type: str
    filename_suffix: str
    endpoint: str | None = None
    endpoint_env: str | None = None
    credential_env: tuple[str, ...] = ()
    options: dict[str, object] = Field(default_factory=dict)

    @field_validator("start_at")
    @classmethod
    def start_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("stream start_at must be timezone-aware UTC")
        return value

    @model_validator(mode="after")
    def validate_chunk(self) -> "WeatherStreamConfig":
        if self.chunk_minutes < self.step_minutes or self.chunk_minutes % self.step_minutes:
            raise ValueError("chunk_minutes must be a multiple of step_minutes")
        if self.chunking == "calendar_month" and self.start_at.day != 1:
            raise ValueError("calendar_month streams must start on the first day")
        if not self.variables:
            raise ValueError("weather stream must define variables")
        if self.endpoint is None and self.endpoint_env is None:
            raise ValueError("weather stream requires endpoint or endpoint_env")
        return self


class WeatherPipelineConfig(ImmutableModel):
    """Complete, credential-free configuration for one source DAG."""

    contract_version: str
    source_id: str
    source_version: str
    provider: Literal["gsmap", "era5_land", "ifs_openmeteo"]
    schedule: str
    aoi_path: Path
    grid_scope_path: Path
    spatial_scope_name: str
    retention_class: RetentionClass
    retention_days: int | None = Field(default=None, gt=0)
    parser_version: str
    license_id: str
    license_uri: str
    max_objects_per_run: int = Field(gt=0)
    streams: tuple[WeatherStreamConfig, ...]

    @model_validator(mode="after")
    def unique_streams(self) -> "WeatherPipelineConfig":
        identities = [stream.stream_id for stream in self.streams]
        if not identities or len(identities) != len(set(identities)):
            raise ValueError("weather stream IDs must be non-empty and unique")
        if self.retention_class == "transient_7d" and self.retention_days != 7:
            raise ValueError("transient_7d weather sources require retention_days=7")
        if self.retention_class == "durable" and self.retention_days is not None:
            raise ValueError("durable weather sources cannot expire")
        for path in (self.aoi_path, self.grid_scope_path):
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("weather scope paths must be project-relative")
        if not self.spatial_scope_name.strip():
            raise ValueError("weather spatial_scope_name cannot be empty")
        return self


class PlannedWeatherObject(ImmutableModel):
    """Small provider request description safe to pass through Airflow XCom."""

    source_id: str
    source_version: str
    spatial_scope_id: str
    stream_id: str
    product: str
    asset_id: str
    window: WeatherWindow
    variables: tuple[str, ...]
    request_fingerprint: str
    source_cycle_id: str
    source_revision: int = Field(default=0, ge=0)
    model_run_time: datetime | None = None
    options: dict[str, object] = Field(default_factory=dict)


class FetchedWeatherObject(ImmutableModel):
    """Downloaded provider payload staged locally for immutable publication."""

    planned: PlannedWeatherObject
    path: Path
    filename: str
    media_type: str
    source_uri: str
    retrieved_at: datetime
    available_at: datetime
    provider_issued_at: datetime | None = None
    provider_metadata: dict[str, object] = Field(default_factory=dict)


class ScopedWeatherObject(ImmutableModel):
    """AOI-only provider payload with evidence for the discarded source response."""

    planned: PlannedWeatherObject
    path: Path
    filename: str
    media_type: str
    source_uri: str
    retrieved_at: datetime
    available_at: datetime
    provider_issued_at: datetime | None = None
    provider_metadata: dict[str, object] = Field(default_factory=dict)
    spatial_scope_id: str
    source_grid_version: str
    cell_indices: tuple[int, ...]
    provider_payload_checksum: str
    provider_payload_size_bytes: int = Field(ge=0)
    scoped_payload_checksum: str
    scoped_payload_size_bytes: int = Field(ge=0)


class PublishedWeatherObject(ImmutableModel):
    """Registered Raw object summarized for coverage and cleanup tasks."""

    source_id: str
    stream_id: str
    product: str
    object_id: str
    asset_id: str
    window: WeatherWindow
    object_uri: str
    snapshot_id: int | None = None
    reused: bool = False
    status: Literal["available", "no_data"] = "available"


class IngestWatermark(ImmutableModel):
    """Durable operational cursor for one provider stream."""

    source_id: str
    product: str
    stream_id: str
    spatial_scope_id: str
    cursor_time: datetime
    last_safe_end: datetime
    last_run_id: str
    status: Literal["ready", "gap", "failed"]
    updated_at: datetime
    detail_json: str = "{}"

    @field_validator("spatial_scope_id")
    @classmethod
    def spatial_scope_must_not_be_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("watermark spatial_scope_id cannot be empty")
        return value

    @field_validator("cursor_time", "last_safe_end", "updated_at")
    @classmethod
    def timestamps_must_be_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("watermark timestamps must be timezone-aware UTC")
        return value


class ObjectLifecycleRow(ImmutableModel):
    """Current storage state for an immutable Raw provenance object."""

    object_id: str
    retention_class: RetentionClass
    storage_status: StorageStatus
    expires_at: datetime | None = None
    bronze_snapshot_id: int | None = Field(default=None, ge=0)
    quality_status: str
    lineage_edge_id: str | None = None
    deleted_at: datetime | None = None
    last_checked_at: datetime
    reason: str | None = None

    @field_validator("expires_at", "deleted_at", "last_checked_at")
    @classmethod
    def lifecycle_timestamps_must_be_utc(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value)
        ):
            raise ValueError("lifecycle timestamps must be timezone-aware UTC")
        return value

    @model_validator(mode="after")
    def validate_expiry_policy(self) -> "ObjectLifecycleRow":
        if self.retention_class == "transient_7d" and self.expires_at is None:
            raise ValueError("transient lifecycle requires expires_at")
        if self.retention_class == "durable" and self.expires_at is not None:
            raise ValueError("durable lifecycle cannot expire")
        return self


class WeatherRasterSlice(ImmutableModel):
    """One weather field and validity window stored as parallel cell arrays."""

    slice_id: str
    object_id: str
    source_id: str
    source_product: str
    source_grid_version: str
    spatial_scope_id: str
    variable: str
    vertical_level: str
    source_cycle_id: str
    model_run_time: datetime | None = None
    valid_time: datetime
    window_start: datetime
    window_end: datetime
    source_revision: int = Field(ge=0)
    cell_indices: tuple[int, ...]
    values: tuple[float | None, ...]
    unit: str
    value_kind: str
    available_at: datetime | None = None
    ingest_run_id: str
    parser_version: str
    quality_status: Literal["passed", "warning", "failed"]

    @field_validator(
        "model_run_time", "valid_time", "window_start", "window_end", "available_at"
    )
    @classmethod
    def slice_timestamps_must_be_utc(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value)
        ):
            raise ValueError("weather slice timestamps must be timezone-aware UTC")
        return value

    @model_validator(mode="after")
    def validate_parallel_arrays_and_window(self) -> "WeatherRasterSlice":
        if len(self.cell_indices) != len(self.values):
            raise ValueError("cell_indices and values must have the same length")
        if not self.cell_indices:
            raise ValueError("weather raster slice cannot be empty")
        if any(index < 0 for index in self.cell_indices):
            raise ValueError("cell_indices cannot be negative")
        if tuple(sorted(set(self.cell_indices))) != self.cell_indices:
            raise ValueError("cell_indices must be sorted and unique")
        if self.window_end < self.window_start:
            raise ValueError("weather slice window_end cannot precede window_start")
        if not self.window_start <= self.valid_time <= self.window_end:
            raise ValueError("valid_time must fall within the weather slice window")
        return self
