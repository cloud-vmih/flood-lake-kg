"""Validated, credential-free settings for Silver weather aggregation."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_TARGET_SEMANTICS = {
    "precipitation": ("mm", "interval_sum"),
    "runoff": ("mm", "interval_sum"),
    "surface_runoff": ("mm", "interval_sum"),
    "subsurface_runoff": ("mm", "interval_sum"),
    "soil_moisture": ("m3/m3", "instantaneous"),
}


class WeatherVariablePolicy(BaseModel):
    """Canonical Silver unit and spatial aggregation for one weather variable."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    unit: str
    aggregation: Literal["area_mean"]
    value_kind: Literal["interval_sum", "instantaneous"]

    @field_validator("unit")
    @classmethod
    def require_unit(cls, value: str) -> str:
        unit = value.strip()
        if not unit:
            raise ValueError("weather variable unit must not be empty")
        return unit


class WeatherAggregationConfig(BaseModel):
    """Input tables, area policy, coverage gate, and canonical output variables."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    pipeline_id: Literal["silver_weather_aggregation"]
    contract_version: str
    weather_table: Literal["bronze.weather_raster_slice"]
    grid_table: Literal["silver.source_grid"]
    basin_table: Literal["silver.dim_basin"]
    weight_table: Literal["silver.grid_basin_weight"]
    output_table: Literal["silver.basin_weather_value"]
    equal_area_crs: Literal["EPSG:6933"]
    geometry_processing_version: str
    quality_policy_id: str
    min_valid_coverage_fraction: float = Field(gt=0, le=1, allow_inf_nan=False)
    variables: dict[str, WeatherVariablePolicy]

    @field_validator("contract_version", "geometry_processing_version", "quality_policy_id")
    @classmethod
    def require_nonempty_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("weather aggregation identifiers must not be empty")
        return normalized

    @model_validator(mode="after")
    def require_canonical_variable_policies(self) -> WeatherAggregationConfig:
        missing = _TARGET_SEMANTICS.keys() - self.variables.keys()
        if missing:
            raise ValueError(f"missing variable policies: {', '.join(sorted(missing))}")
        for name, (unit, value_kind) in _TARGET_SEMANTICS.items():
            policy = self.variables[name]
            if (policy.unit, policy.value_kind) != (unit, value_kind):
                raise ValueError(f"{name} must use unit={unit} and value_kind={value_kind}")
        return self


def load_weather_aggregation_config(path: Path) -> WeatherAggregationConfig:
    """Read a YAML policy and reject malformed or incomplete weather settings."""
    document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise TypeError("weather aggregation config must be a mapping")
    return WeatherAggregationConfig.model_validate(document)
