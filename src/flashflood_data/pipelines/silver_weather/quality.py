"""Silver weather rules compiled into the shared Spark DQ framework."""

from __future__ import annotations

from flashflood_data.pipelines.silver_weather.config import WeatherAggregationConfig
from flashflood_data.shared.spark.quality import (
    SparkQualityPlan,
    SparkQualityRule,
    compile_quality_plan,
)


def weight_quality_plan() -> SparkQualityPlan:
    return compile_quality_plan(
        (
            SparkQualityRule(
                "weight_identity", "v1", "fatal",
                "source_id IS NOT NULL AND source_grid_version IS NOT NULL AND "
                "source_grid_id IS NOT NULL AND basin_id IS NOT NULL AND "
                "basin_version IS NOT NULL AND geometry_processing_version IS NOT NULL",
            ),
            SparkQualityRule(
                "weight_area", "v1", "fatal",
                "intersection_area_m2 > 0 AND NOT isnan(intersection_area_m2)",
            ),
            SparkQualityRule(
                "weight_fraction", "v1", "fatal",
                "weight_by_basin > 0 AND weight_by_basin <= 1 AND "
                "weight_by_grid > 0 AND weight_by_grid <= 1",
            ),
        )
    )


def weather_quality_plan(config: WeatherAggregationConfig) -> SparkQualityPlan:
    unit_kind = " OR ".join(
        "(variable = '" + name.replace("'", "''") + "' AND unit = '"
        + policy.unit.replace("'", "''") + "' AND value_kind = '"
        + policy.value_kind + "')"
        for name, policy in sorted(config.variables.items())
    )
    threshold = repr(config.min_valid_coverage_fraction)
    return compile_quality_plan(
        (
            SparkQualityRule(
                "weather_identity", "v1", "fatal",
                "business_key_hash IS NOT NULL AND basin_id IS NOT NULL AND "
                "basin_version IS NOT NULL AND source_id IS NOT NULL AND "
                "source_slice_id IS NOT NULL AND available_at IS NOT NULL",
            ),
            SparkQualityRule(
                "weather_window", "v1", "fatal",
                "(value_kind = 'instantaneous' AND window_end >= window_start) OR "
                "(value_kind <> 'instantaneous' AND window_end > window_start)",
            ),
            SparkQualityRule(
                "weather_coverage", "v1", "fatal",
                "valid_coverage_fraction >= 0 AND valid_coverage_fraction <= 1 "
                "AND NOT isnan(valid_coverage_fraction)",
            ),
            SparkQualityRule("weather_unit_kind", "v1", "fatal", unit_kind),
            SparkQualityRule(
                "weather_physical_range", "v1", "reject",
                "value IS NULL OR (NOT isnan(value) AND "
                "ABS(value) < CAST('Infinity' AS DOUBLE) AND "
                "((variable = 'soil_moisture' AND value >= 0 AND value <= 1) "
                "OR (variable <> 'soil_moisture' AND value >= 0)))",
            ),
            SparkQualityRule(
                "weather_low_coverage", "v1", "warning",
                f"valid_coverage_fraction >= {threshold}",
            ),
        )
    )
