"""Normalize source-faithful Bronze weather slices for Silver aggregation.

The result remains one row per slice with aligned cell arrays. No provider
payload is downloaded and no Iceberg table is written here.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from flashflood_data.pipelines.silver_weather.config import WeatherAggregationConfig

_ERA5_FLUX = ("total_precipitation", "surface_runoff", "sub_surface_runoff")
_ERA5_SOIL = tuple(f"volumetric_soil_water_layer_{index}" for index in range(1, 5))
_IFS_SOIL = (
    "soil_moisture_0_to_7cm",
    "soil_moisture_7_to_28cm",
    "soil_moisture_28_to_100cm",
    "soil_moisture_100_to_255cm",
)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _list(values: tuple[str, ...]) -> str:
    return ", ".join(map(_literal, values))


def normalize_weather_slices(
    spark: Any,
    bronze_slices: Any,
    config: WeatherAggregationConfig,
    *,
    batch_month: str | None = None,
) -> Any:
    """Return a lazy DataFrame with canonical variables, units, and windows.

    ERA5 hourly increments require the immediately preceding accumulation on
    the same source grid, scope, and variable. Revisions must match within a
    monthly source cycle; the first 00 UTC sample may use the preceding month's
    cycle, whose file checksum revision is necessarily independent. A missing predecessor
    produces null cell values and a warning, never a fabricated hourly value.
    The Spark session must use UTC (as configured by shared/spark/session.py).
    """
    required = {"precipitation", "runoff", "surface_runoff", "subsurface_runoff", "soil_moisture"}
    if not required <= set(config.variables):
        raise ValueError("Silver weather policy lacks source normalization targets")

    batch_filter = ""
    source_filter = ""
    if batch_month is not None:
        if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", batch_month):
            raise ValueError("invalid weather batch_month")
        year, month = map(int, batch_month.split("-"))
        start = date(year, month, 1)
        end = date(year + (month == 12), month % 12 + 1, 1)
        batch_filter = (
            f"WHERE s.valid_time >= TIMESTAMP '{start.isoformat()} 00:00:00' "
            f"AND s.valid_time < TIMESTAMP '{end.isoformat()} 00:00:00'"
        )
        predecessor_start = datetime(
            year, month, 1, tzinfo=timezone.utc  # noqa: UP017 - Spark image uses Python 3.10.
        ) - timedelta(hours=1)
        source_filter = (
            f"AND s.valid_time >= TIMESTAMP '{predecessor_start:%Y-%m-%d %H:%M:%S}' "
            f"AND s.valid_time < TIMESTAMP '{end.isoformat()} 00:00:00'"
        )

    view = f"bronze_weather_{uuid4().hex}"
    gsmap = (
        "c.source_id = 'gsmap' AND c.variable = 'precipitation' "
        "AND c.unit = 'mm/h' AND c.value_kind = 'rate' "
        "AND c.window_end > c.window_start"
    )
    era5_flux = (
        "c.source_id = 'era5_land' "
        f"AND c.variable IN ({_list(_ERA5_FLUX)}) "
        "AND c.unit = 'm' AND c.value_kind = 'accumulation_since_00_utc'"
    )
    era5_soil = (
        "c.source_id = 'era5_land' "
        f"AND c.variable IN ({_list(_ERA5_SOIL)}) "
        "AND c.unit IN ('m3 m-3', 'm**3 m**-3', 'm3/m3') "
        "AND c.value_kind = 'instantaneous'"
    )
    ifs_flux = (
        "c.source_id = 'ifs_openmeteo' AND c.variable IN ('precipitation', 'runoff') "
        "AND c.unit = 'mm' AND c.value_kind = 'preceding_hour_sum' "
        "AND c.window_end > c.window_start"
    )
    ifs_soil = (
        "c.source_id = 'ifs_openmeteo' "
        f"AND c.variable IN ({_list(_IFS_SOIL)}) "
        "AND c.unit IN ('m3/m3', 'm³/m³', 'm3 m-3') "
        "AND c.value_kind = 'instantaneous'"
    )
    supported = " OR ".join(f"({rule})" for rule in (gsmap, era5_flux, era5_soil, ifs_flux, ifs_soil))

    # ERA5-Land marks the 00 UTC value as the previous day's 24-hour total.
    # At 01 UTC the accumulator has reset; that value is already one hour.
    # Every other hour needs a contiguous predecessor, including 00 UTC.
    era5_increment = """
        CASE
          WHEN hour(c.valid_time) = 1 THEN
            transform(c.values, v ->
                CASE WHEN v IS NULL OR CAST(v AS DOUBLE) < -0.000000001
                     THEN CAST(NULL AS DOUBLE)
                     ELSE GREATEST(0.0, CAST(v AS DOUBLE) * 1000.0) END)
          WHEN p.previous_slice_id IS NOT NULL AND c.cell_indices = p.previous_indices THEN
            zip_with(c.values, p.previous_values, (v, previous_v) ->
                CASE WHEN v IS NULL OR previous_v IS NULL OR
                          CAST(v AS DOUBLE) - CAST(previous_v AS DOUBLE) < -0.000000001
                     THEN CAST(NULL AS DOUBLE)
                     ELSE GREATEST(0.0,
                          (CAST(v AS DOUBLE) - CAST(previous_v AS DOUBLE)) * 1000.0) END)
          ELSE transform(c.values, v -> CAST(NULL AS DOUBLE))
        END
    """
    gsmap_amount = """
        transform(c.values, v ->
            CASE WHEN v IS NULL THEN CAST(NULL AS DOUBLE)
                 ELSE CAST(v AS DOUBLE) *
                      (unix_micros(c.window_end) - unix_micros(c.window_start)) /
                      3600000000.0 END)
    """
    missing_era5_predecessor = (
        "hour(c.valid_time) <> 1 AND "
        "(p.previous_slice_id IS NULL OR c.cell_indices <> p.previous_indices)"
    )

    created = False
    try:
        bronze_slices.createOrReplaceTempView(view)
        created = True
        return spark.sql(
            f"""
            WITH source_rows AS (
                SELECT * FROM {view} s
                WHERE 1 = 1 {source_filter}
                  AND assert_true(size(s.cell_indices) = size(s.values),
                                  'Bronze weather arrays differ in length') IS NULL
                  AND assert_true(size(s.cell_indices) > 0,
                                  'Bronze weather slice has no cells') IS NULL
                  AND assert_true(
                      size(array_distinct(s.cell_indices)) = size(s.cell_indices),
                      'Bronze weather cell indices are duplicated') IS NULL
            ),
            current_rows AS (
                SELECT * FROM source_rows s {batch_filter}
            ),
            era5_candidates AS (
                SELECT c.slice_id AS current_slice_id,
                       p.slice_id AS previous_slice_id,
                       p.cell_indices AS previous_indices,
                       p.values AS previous_values,
                       p.available_at AS previous_available_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY c.slice_id
                           ORDER BY CASE WHEN p.source_cycle_id = c.source_cycle_id
                                         THEN 0 ELSE 1 END,
                                    p.available_at DESC NULLS LAST, p.slice_id DESC
                       ) AS predecessor_rank
                FROM current_rows c
                LEFT JOIN source_rows p
                  ON p.source_id = c.source_id
                 AND p.source_product = c.source_product
                 AND p.source_grid_version = c.source_grid_version
                 AND p.spatial_scope_id = c.spatial_scope_id
                 AND p.variable = c.variable
                 AND p.vertical_level = c.vertical_level
                 AND (
                     p.source_revision = c.source_revision OR
                     (p.source_cycle_id <> c.source_cycle_id
                      AND dayofmonth(c.valid_time) = 1 AND hour(c.valid_time) = 0)
                 )
                 AND p.valid_time = c.valid_time - INTERVAL 1 HOUR
                WHERE {era5_flux}
            ),
            era5_previous AS (
                SELECT * FROM era5_candidates WHERE predecessor_rank = 1
            )
            SELECT c.slice_id, c.object_id, c.source_id, c.source_product,
                   c.source_grid_version, c.spatial_scope_id,
                   CASE WHEN {era5_flux} AND c.variable = 'total_precipitation'
                            THEN 'precipitation'
                        WHEN {era5_flux} AND c.variable = 'sub_surface_runoff'
                            THEN 'subsurface_runoff'
                        WHEN {era5_soil} OR {ifs_soil} THEN 'soil_moisture'
                        ELSE c.variable END AS variable,
                   c.vertical_level, c.source_cycle_id, c.model_run_time,
                   c.valid_time,
                   CASE WHEN {era5_flux} THEN c.valid_time - INTERVAL 1 HOUR
                        ELSE c.window_start END AS window_start,
                   CASE WHEN {era5_flux} THEN c.valid_time
                        ELSE c.window_end END AS window_end,
                   c.source_revision, c.cell_indices,
                   CASE WHEN {gsmap} THEN {gsmap_amount}
                        WHEN {era5_flux} THEN {era5_increment}
                        ELSE transform(c.values, v -> CAST(v AS DOUBLE))
                   END AS values,
                   CASE WHEN {era5_soil} OR {ifs_soil} THEN 'm3/m3'
                        ELSE 'mm' END AS unit,
                   CASE WHEN {era5_soil} OR {ifs_soil} THEN 'instantaneous'
                        ELSE 'interval_sum' END AS value_kind,
                   CASE WHEN {era5_flux} AND hour(c.valid_time) <> 1 AND
                                  p.previous_available_at IS NOT NULL
                        THEN GREATEST(c.available_at, p.previous_available_at)
                        ELSE c.available_at END AS available_at,
                   c.ingest_run_id, c.parser_version,
                   CASE WHEN {era5_flux} AND ({missing_era5_predecessor})
                        THEN 'warning' ELSE c.quality_status END AS quality_status,
                   CASE WHEN {era5_flux} AND hour(c.valid_time) <> 1 AND
                                  p.previous_slice_id IS NOT NULL AND
                                  c.cell_indices = p.previous_indices
                        THEN p.previous_slice_id ELSE NULL
                   END AS normalization_previous_slice_id
            FROM current_rows c
            LEFT JOIN era5_previous p ON p.current_slice_id = c.slice_id
            WHERE assert_true({supported},
                              'unsupported Bronze weather variable, unit, or value kind') IS NULL
            """
        )
    finally:
        if created:
            spark.catalog.dropTempView(view)
