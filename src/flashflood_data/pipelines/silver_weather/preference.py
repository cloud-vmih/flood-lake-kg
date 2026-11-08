"""Keep ERA5 rain only where no valid GSMaP observation covers the exact window."""

from __future__ import annotations

import re
from datetime import date
from typing import Any
from uuid import uuid4

from flashflood_data.pipelines.silver_weather.config import WeatherAggregationConfig

_WINDOW_KEY = (
    "basin_id", "basin_version", "window_start", "window_end", "variable",
    "vertical_level", "unit", "value_kind",
)


def _valid_gsmap(alias: str, config: WeatherAggregationConfig) -> str:
    return (
        f"{alias}.source_id = 'gsmap' AND {alias}.variable = 'precipitation' "
        f"AND {alias}.unit = 'mm' AND {alias}.value_kind = 'interval_sum' "
        f"AND {alias}.window_end > {alias}.window_start "
        f"AND {alias}.value IS NOT NULL AND NOT isnan({alias}.value) "
        f"AND {alias}.value >= 0 AND {alias}.value < CAST('Infinity' AS DOUBLE) "
        f"AND {alias}.valid_coverage_fraction >= {config.min_valid_coverage_fraction!r} "
        f"AND {alias}.valid_coverage_fraction <= 1"
    )


def _same_window(left: str, right: str) -> str:
    return " AND ".join(f"{left}.{name} = {right}.{name}" for name in _WINDOW_KEY)


def prefer_gsmap(
    spark: Any, candidates: Any, existing: Any, config: WeatherAggregationConfig,
    *, batch_month: str | None = None,
) -> Any:
    """Filter validated candidates; retain NOW/Standard and all non-ERA5-rain rows.

    Existing Silver rows participate so replaying an ERA5-only batch cannot
    reintroduce fallback rain where GSMaP was already published. Zero rainfall
    is valid. Low coverage, null/nonfinite/negative values and shifted windows
    cannot suppress ERA5. Source revisions remain separate observations.
    Existing GSMaP keys being replaced by this publish are excluded: their
    pending replacement, rather than their old value, decides eligibility.
    """
    views = [f"weather_preference_{uuid4().hex}" for _ in range(2)]
    created = []
    try:
        for name, frame in zip(views, (candidates, existing), strict=True):
            frame.createOrReplaceTempView(name)
            created.append(name)
        keys = ", ".join(f"g.{name}" for name in _WINDOW_KEY)
        month = f" AND {_month_filter(batch_month, 'g')}" if batch_month is not None else ""
        return spark.sql(f"""
            WITH gsmap_keys AS (
                SELECT {keys} FROM {views[0]} g WHERE {_valid_gsmap('g', config)}
                UNION
                SELECT {keys} FROM {views[1]} g
                LEFT ANTI JOIN {views[0]} incoming
                  ON g.business_key_hash = incoming.business_key_hash
                WHERE {_valid_gsmap('g', config)} {month}
            )
            SELECT c.* FROM {views[0]} c
            LEFT ANTI JOIN gsmap_keys g
              ON c.source_id = 'era5_land' AND c.variable = 'precipitation'
             AND {_same_window('c', 'g')}
        """)
    finally:
        for name in reversed(created):
            spark.catalog.dropTempView(name)


def _month_filter(batch_month: str, alias: str) -> str:
    if re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", batch_month) is None:
        raise ValueError("invalid weather batch_month")
    year, month = map(int, batch_month.split("-"))
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return (
        f"{alias}.valid_time >= TIMESTAMP '{start.isoformat()} 00:00:00' "
        f"AND {alias}.valid_time < TIMESTAMP '{end.isoformat()} 00:00:00'"
    )


def remove_superseded_era5(
    spark: Any, table: str, config: WeatherAggregationConfig, *, batch_month: str,
) -> int:
    """Delete fallback rain only after its GSMaP replacement is durable in Silver.

    One bounded MERGE deletes obsolete ERA5 keys for this month. Materializing
    the keys cuts the self-reference to the Iceberg target. Repeating the
    cleanup with no obsolete keys creates no new snapshot. The caller writes
    its success manifest after this operation, so interruption is repairable.
    """
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){1,2}", table) is None:
        raise ValueError("unsafe weather table identifier")
    target = ".".join(f"`{part}`" for part in table.split("."))
    month = _month_filter(batch_month, "t")
    keys = spark.sql(f"""
        SELECT DISTINCT t.business_key_hash
        FROM {target} t
        INNER JOIN {target} g ON {_same_window('t', 'g')}
        WHERE t.source_id = 'era5_land' AND t.variable = 'precipitation'
          AND {month} AND {_month_filter(batch_month, 'g')}
          AND {_valid_gsmap('g', config)}
    """).checkpoint(eager=True)
    count = keys.count()
    if not count:
        return 0
    view = f"weather_superseded_{uuid4().hex}"
    keys.createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            MERGE INTO {target} t USING {view} s
            ON t.business_key_hash = s.business_key_hash
               AND t.source_id = 'era5_land' AND t.variable = 'precipitation'
               AND {month}
            WHEN MATCHED THEN DELETE
        """)
    finally:
        spark.catalog.dropTempView(view)
    return count
