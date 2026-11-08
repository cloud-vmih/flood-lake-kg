"""Deterministic Silver weather build decisions and versioned row selection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Literal

from flashflood_data.shared.spark.models import PinnedSnapshot
from flashflood_data.shared.spark.snapshots import BuildRecord, plan_incremental_build

JOB_NAME = "silver_weather_aggregation"
ALGORITHM_VERSION = "weather-normalize-aggregate-v3"
WEATHER_OUTPUTS = ("silver.grid_basin_weight", "silver.basin_weather_value")
WEIGHT_KEY = (
    "source_id", "source_grid_version", "source_grid_id", "basin_id",
    "basin_version", "geometry_processing_version",
)
WEATHER_KEY = ("business_key_hash",)


@dataclass(frozen=True)
class WeatherBuildPlan:
    action: Literal["build", "repair", "skip"]
    pipeline_run_id: str
    build_signature: str
    snapshots: tuple[PinnedSnapshot, ...]
    missing_outputs: tuple[str, ...]
    config_hash: str
    prior_run_id: str | None = None

    def to_document(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> WeatherBuildPlan:
        values = dict(document)
        values["snapshots"] = tuple(PinnedSnapshot(**row) for row in values["snapshots"])
        values["missing_outputs"] = tuple(values["missing_outputs"])
        plan = cls(**values)
        if not set(plan.missing_outputs) <= set(WEATHER_OUTPUTS):
            raise ValueError("weather plan contains an unknown output table")
        return plan


def plan_weather_build(
    snapshots: Sequence[PinnedSnapshot],
    *,
    config_hash: str,
    history: Sequence[BuildRecord],
) -> WeatherBuildPlan:
    required = {"bronze.weather_raster_slice", "silver.source_grid", "silver.dim_basin"}
    if {item.table for item in snapshots} != required or len(snapshots) != 3:
        raise ValueError("weather build requires exactly three pinned input tables")
    incremental = plan_incremental_build(
        snapshots=snapshots,
        config_hash=config_hash,
        policy_id="silver-weather-v1",
        algorithm_version=ALGORITHM_VERSION,
        contract_version="1.0",
        required_outputs=WEATHER_OUTPUTS,
        history=tuple(
            record for record in history
            if record.status in {"succeeded", "succeeded_with_warning"}
        ),
    )
    return WeatherBuildPlan(
        action=incremental.action,
        pipeline_run_id=f"silver-weather-{incremental.build_signature[:24]}",
        build_signature=incremental.build_signature,
        snapshots=tuple(sorted(snapshots)),
        missing_outputs=incremental.missing_outputs,
        config_hash=config_hash,
        prior_run_id=incremental.prior_run_id,
    )


def missing_weight_rows(candidate: Any, existing: Any | None) -> Any:
    """Keep only exact grid/basin/algorithm combinations not already stored."""
    if existing is None:
        return candidate
    return candidate.join(existing.select(*WEIGHT_KEY).distinct(), on=list(WEIGHT_KEY), how="left_anti")
