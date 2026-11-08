"""Lazy Spark aggregation of canonical weather cell values into basin values."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from flashflood_data.pipelines.silver_weather.config import WeatherAggregationConfig
from flashflood_data.pipelines.silver_weather.revisions import weather_business_key_sql


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _policy_case(config: WeatherAggregationConfig, field: str) -> str:
    branches = " ".join(
        f"WHEN {_literal(variable)} THEN {_literal(getattr(policy, field))}"
        for variable, policy in sorted(config.variables.items())
    )
    return f"CASE s.variable {branches} ELSE NULL END"


def aggregate_weather_slices(
    spark: Any,
    weather_rows: Any,
    grid_rows: Any,
    weight_rows: Any,
    config: WeatherAggregationConfig,
) -> Any:
    """Return a lazy basin value frame for already normalized weather slices.

    ``weather_rows`` must use the canonical variable, unit, value kind, and
    interval window from ``config``. In particular, source rates and ERA5
    cumulative values must be converted before this spatial aggregation. The
    SQL fails on malformed alignment or unsupported semantics rather than
    publishing a plausible but incorrect number. DQ and Iceberg publish are
    owned by subsequent pipeline steps.
    """
    views = {
        "weather": f"weather_slices_{uuid4().hex}",
        "grid": f"weather_cells_{uuid4().hex}",
        "weights": f"weather_weights_{uuid4().hex}",
    }
    created: list[str] = []
    threshold = repr(config.min_valid_coverage_fraction)
    geometry_version = _literal(config.geometry_processing_version)
    unit_case = _policy_case(config, "unit")
    kind_case = _policy_case(config, "value_kind")
    output_unit_case = unit_case.replace("s.variable", "v.variable")
    output_kind_case = kind_case.replace("s.variable", "v.variable")
    key_expression = weather_business_key_sql("v")
    try:
        for label, frame in (
            ("weather", weather_rows),
            ("grid", grid_rows),
            ("weights", weight_rows),
        ):
            frame.createOrReplaceTempView(views[label])
            created.append(views[label])

        return spark.sql(
            f"""
            WITH validated_slices AS (
                SELECT * FROM {views['weather']} s
                WHERE assert_true(size(s.cell_indices) = size(s.values),
                                  'weather arrays differ in length') IS NULL
                  AND assert_true(size(s.cell_indices) > 0,
                                  'weather slice has no cells') IS NULL
                  AND assert_true(
                      size(array_distinct(s.cell_indices)) = size(s.cell_indices),
                      'weather slice has duplicate cell indices') IS NULL
                  AND assert_true(
                      s.window_end > s.window_start OR
                      (s.value_kind = 'instantaneous' AND s.window_end = s.window_start),
                                  'weather window is invalid') IS NULL
                  AND assert_true(s.source_revision >= 0,
                                  'weather revision is invalid') IS NULL
                  AND assert_true(s.available_at IS NOT NULL,
                                  'weather availability is missing') IS NULL
                  AND assert_true(s.variable IN ({', '.join(_literal(v) for v in sorted(config.variables))}),
                                  'weather variable is not canonical') IS NULL
                  AND assert_true(s.unit = {unit_case},
                                  'weather unit is not canonical') IS NULL
                  AND assert_true(s.value_kind = {kind_case},
                                  'weather value kind is not canonical') IS NULL
            ),
            exploded AS (
                SELECT s.slice_id, s.source_id, s.source_product,
                       s.source_grid_version, s.source_cycle_id,
                       s.model_run_time, s.valid_time, s.window_start, s.window_end,
                       s.variable, s.vertical_level, s.source_revision,
                       s.available_at, cell_index,
                       CAST(element_at(s.values, cell_pos + 1) AS DOUBLE) AS cell_value
                FROM validated_slices s
                LATERAL VIEW posexplode(s.cell_indices) cells AS cell_pos, cell_index
            ),
            source_cells AS (
                SELECT e.*, g.source_grid_id
                FROM exploded e
                LEFT JOIN {views['grid']} g
                  ON g.cell_index = e.cell_index
                 AND g.source_id = e.source_id
                 AND g.source_grid_version = e.source_grid_version
                WHERE assert_true(g.source_grid_id IS NOT NULL,
                                  'weather cell_index is absent from source_grid') IS NULL
            ),
            weighted AS (
                SELECT c.*, w.basin_id, w.basin_version, w.weight_by_basin
                FROM source_cells c
                INNER JOIN {views['weights']} w
                  ON w.source_id = c.source_id
                 AND w.source_grid_version = c.source_grid_version
                 AND w.source_grid_id = c.source_grid_id
                 AND w.geometry_processing_version = {geometry_version}
            ),
            grouped AS (
                SELECT slice_id, source_id, source_product, source_grid_version,
                       source_cycle_id, model_run_time, valid_time,
                       window_start, window_end, variable, vertical_level,
                       source_revision, available_at, basin_id, basin_version,
                       SUM(weight_by_basin) AS total_weight,
                       SUM(CASE WHEN cell_value IS NOT NULL AND NOT isnan(cell_value)
                                     AND ABS(cell_value) < CAST('Infinity' AS DOUBLE)
                                THEN weight_by_basin ELSE 0.0 END) AS valid_weight,
                       SUM(CASE WHEN cell_value IS NOT NULL AND NOT isnan(cell_value)
                                     AND ABS(cell_value) < CAST('Infinity' AS DOUBLE)
                                THEN cell_value * weight_by_basin ELSE 0.0 END)
                           AS weighted_sum
                FROM weighted
                GROUP BY slice_id, source_id, source_product, source_grid_version,
                         source_cycle_id, model_run_time, valid_time,
                         window_start, window_end, variable, vertical_level,
                         source_revision, available_at, basin_id, basin_version
            ),
            values_by_basin AS (
                SELECT *,
                       CASE WHEN valid_weight >= {threshold} AND valid_weight > 0
                            THEN weighted_sum / valid_weight ELSE NULL END AS value,
                       LEAST(1.0, valid_weight) AS valid_coverage_fraction,
                       to_json(named_struct(
                           'low_coverage', valid_weight < {threshold},
                           'missing_cells', valid_weight < total_weight,
                           'weight_sum_exceeds_one', total_weight > 1.00000001
                       )) AS quality_flags_json
                FROM grouped
            )
            SELECT {key_expression} AS business_key_hash,
                   v.basin_id, v.basin_version, v.source_id, v.source_product,
                   v.source_grid_version, v.slice_id AS source_slice_id,
                   v.source_cycle_id, v.model_run_time, v.valid_time,
                   v.window_start, v.window_end, v.variable, v.vertical_level,
                   v.value, {output_unit_case} AS unit,
                   {output_kind_case} AS value_kind,
                   v.source_revision, v.available_at,
                   v.valid_coverage_fraction, v.quality_flags_json
            FROM values_by_basin v
            """
        )
    finally:
        for view in reversed(created):
            spark.catalog.dropTempView(view)
