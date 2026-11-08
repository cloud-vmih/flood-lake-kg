"""Validated records produced by the Silver weather aggregation pipeline."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10.


class GridBasinWeightRow(BaseModel):
    """Area overlap for one versioned provider cell and one versioned basin."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    source_id: str
    source_grid_version: str
    source_grid_id: str
    basin_id: str
    basin_version: str
    geometry_processing_version: str
    intersection_area_m2: float = Field(gt=0, allow_inf_nan=False)
    weight_by_basin: float = Field(gt=0, le=1, allow_inf_nan=False)
    weight_by_grid: float = Field(gt=0, le=1, allow_inf_nan=False)
    quality_status: Literal["passed", "warning"]

    @field_validator(
        "source_id",
        "source_grid_version",
        "source_grid_id",
        "basin_id",
        "basin_version",
        "geometry_processing_version",
    )
    @classmethod
    def require_identity(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("grid and basin identities must not be empty")
        return normalized


class BasinWeatherValueRow(BaseModel):
    """One source slice aggregated for one basin and its geometry version."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    business_key_hash: str
    basin_id: str
    basin_version: str
    source_id: str
    source_product: str
    source_grid_version: str
    source_slice_id: str
    source_cycle_id: str
    model_run_time: datetime | None = None
    valid_time: datetime
    window_start: datetime
    window_end: datetime
    variable: str
    vertical_level: str
    value: float | None = Field(allow_inf_nan=False)
    unit: str
    value_kind: str
    source_revision: int = Field(ge=0)
    available_at: datetime
    valid_coverage_fraction: float = Field(ge=0, le=1, allow_inf_nan=False)
    quality_flags_json: str

    @field_validator("business_key_hash")
    @classmethod
    def require_business_key_hash(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("business_key_hash must be a lowercase SHA-256 digest")
        return value

    @field_validator(
        "basin_id",
        "basin_version",
        "source_id",
        "source_product",
        "source_grid_version",
        "source_slice_id",
        "source_cycle_id",
        "variable",
        "vertical_level",
        "unit",
        "value_kind",
    )
    @classmethod
    def require_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("weather identity and unit fields must not be empty")
        return normalized

    @field_validator("model_run_time", "valid_time", "window_start", "window_end", "available_at")
    @classmethod
    def require_utc_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() != _UTC.utcoffset(value)):
            raise ValueError("weather timestamps must be timezone-aware UTC")
        return value

    @field_validator("quality_flags_json")
    @classmethod
    def require_json_object(cls, value: str) -> str:
        try:
            flags = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError("quality_flags_json must contain a JSON object") from error
        if not isinstance(flags, dict):
            raise ValueError("quality_flags_json must contain a JSON object")  # noqa: TRY004
        return value

    @model_validator(mode="after")
    def require_ordered_window(self) -> BasinWeatherValueRow:
        if self.value_kind == "instantaneous":
            if self.window_end < self.window_start:
                raise ValueError("window_end must not precede window_start")
        elif self.window_end <= self.window_start:
            raise ValueError("window_end must be after window_start")
        return self
