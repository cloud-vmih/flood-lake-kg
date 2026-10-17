"""Immutable domain records for the Silver L12 basin topology pipeline."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import ConfigDict, Field, field_validator, model_validator

from flashflood_data.catalog.models import ImmutableModel

EdgeStatus = Literal["internal", "exits_AOI", "terminal"]

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class BasinRow(ImmutableModel):
    """One canonical HydroBASINS L12 polygon at one immutable basin version."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    basin_id: str
    basin_version: str
    hydrobasins_level: Literal[12]
    geometry_wkb: bytes
    crs: Literal["EPSG:4326"]
    geometry_hash: str
    next_down_id: str | None = None
    main_basin_id: str | None = None
    pfaf_id: str | None = None
    topology_sort_hint: int | None = None
    basin_area_km2: float = Field(gt=0)
    upstream_area_km2: float = Field(gt=0)
    distance_to_main_sink_km: float | None = Field(default=None, ge=0)
    next_sink_id: str | None = None
    distance_to_sink_km: float | None = Field(default=None, ge=0)
    valid_from: datetime
    valid_to: datetime | None = None
    source_object_id: str

    @field_validator("basin_id", "basin_version", "source_object_id")
    @classmethod
    def require_identity(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("basin identity fields must not be empty")
        return normalized

    @field_validator("next_down_id", "main_basin_id", "pfaf_id", "next_sink_id")
    @classmethod
    def normalize_optional_identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("geometry_wkb")
    @classmethod
    def require_geometry_bytes(cls, value: bytes) -> bytes:
        if not value:
            raise ValueError("geometry_wkb must not be empty")
        return value

    @field_validator("geometry_hash")
    @classmethod
    def require_sha256_geometry_hash(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not _SHA256.fullmatch(normalized):
            raise ValueError("geometry_hash must be a lowercase SHA-256 hex digest")
        return normalized

    @field_validator("valid_from", "valid_to")
    @classmethod
    def require_utc_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("basin validity timestamps must use timezone-aware UTC")
        return value

    @model_validator(mode="after")
    def require_valid_time_range(self) -> BasinRow:
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be later than valid_from")
        return self


class BasinEdgeRow(ImmutableModel):
    """One direct upstream-to-downstream relation for a versioned basin graph."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    topology_version: str
    basin_version: str
    upstream_basin_id: str
    downstream_basin_id: str | None = None
    edge_status: EdgeStatus
    river_length_km: float | None = Field(default=None, ge=0)
    elevation_drop_m: float | None = Field(default=None, ge=0)
    channel_slope_m_m: float | None = Field(default=None, ge=0)
    cycle_check: str
    outlet_check: str
    source_object_id: str

    @field_validator(
        "topology_version",
        "basin_version",
        "upstream_basin_id",
        "cycle_check",
        "outlet_check",
        "source_object_id",
    )
    @classmethod
    def require_nonempty_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("basin edge identity and check fields must not be empty")
        return normalized

    @field_validator("downstream_basin_id")
    @classmethod
    def normalize_downstream_identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def require_status_consistent_downstream(self) -> BasinEdgeRow:
        if self.edge_status == "internal":
            if self.downstream_basin_id is None:
                raise ValueError("internal edge requires downstream_basin_id")
            if self.downstream_basin_id == self.upstream_basin_id:
                raise ValueError("internal edge cannot point to itself")
        elif self.downstream_basin_id is not None:
            raise ValueError("exits_AOI and terminal edges keep downstream_basin_id null")
        return self


class TopologyReport(ImmutableModel):
    """Deterministic graph diagnostics consumed by topology DQ rules."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    basin_count: int = Field(ge=0)
    edge_count: int = Field(ge=0)
    internal_edge_count: int = Field(ge=0)
    exit_edge_count: int = Field(ge=0)
    terminal_edge_count: int = Field(ge=0)
    topological_order: tuple[str, ...] = ()
    cycle_nodes: tuple[str, ...] = ()
    duplicate_basin_ids: tuple[str, ...] = ()
    missing_internal_targets: tuple[str, ...] = ()

    @field_validator(
        "topological_order",
        "cycle_nodes",
        "duplicate_basin_ids",
        "missing_internal_targets",
    )
    @classmethod
    def require_unique_nonempty_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(item.strip() for item in value)
        if any(not item for item in normalized) or len(normalized) != len(set(normalized)):
            raise ValueError("topology report ID collections must contain unique nonempty IDs")
        return normalized

    @model_validator(mode="after")
    def require_consistent_counts(self) -> TopologyReport:
        classified = (
            self.internal_edge_count + self.exit_edge_count + self.terminal_edge_count
        )
        if classified != self.edge_count:
            raise ValueError("edge status counts must sum to edge_count")
        if self.edge_count != self.basin_count:
            raise ValueError("a basin topology build must emit exactly one edge per basin")
        if set(self.topological_order) & set(self.cycle_nodes):
            raise ValueError("cycle nodes cannot appear in the topological order")
        return self
