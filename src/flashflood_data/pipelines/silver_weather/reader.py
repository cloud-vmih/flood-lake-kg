"""Read version-pinned Bronze weather and Silver spatial inputs for one build."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from flashflood_data.pipelines.silver_weather.config import WeatherAggregationConfig
from flashflood_data.shared.spark.models import PinnedSnapshot
from flashflood_data.shared.spark.snapshots import SparkSnapshotManager


@dataclass(frozen=True)
class WeatherSilverInputs:
    """Lazy frames backed by the exact Iceberg snapshots in ``snapshots``."""

    weather: Any
    grid: Any
    basins: Any
    existing_weights: Any | None
    snapshots: tuple[PinnedSnapshot, ...]


def read_weather_silver_inputs(
    spark: Any,
    snapshot_manager: SparkSnapshotManager,
    config: WeatherAggregationConfig,
) -> WeatherSilverInputs:
    """Pin all available inputs once, then read only those immutable versions.

    The weight table is optional on the first build. Its absence means every
    grid/basin/version combination must be computed by the later pipeline step.
    """
    mandatory = (config.weather_table, config.grid_table, config.basin_table)
    has_weights = spark.catalog.tableExists(snapshot_manager.qualify(config.weight_table))
    tables = (*mandatory, config.weight_table) if has_weights else mandatory
    try:
        snapshots = snapshot_manager.pin(spark, tables)
    except ValueError as error:
        # An Iceberg table may have been created before its first commit. It
        # exists in the catalog but has no readable snapshot yet.
        if not has_weights or str(error) != (
            f"Iceberg input table has no snapshot: {config.weight_table}"
        ):
            raise
        has_weights = False
        tables = mandatory
        snapshots = snapshot_manager.pin(spark, tables)
    by_table = {snapshot.table: snapshot for snapshot in snapshots}
    if set(by_table) != set(tables):
        raise ValueError("pinned weather inputs do not match the requested tables")

    def read(table: str) -> Any:
        return snapshot_manager.read(spark, by_table[table])

    return WeatherSilverInputs(
        weather=read(config.weather_table),
        grid=read(config.grid_table),
        basins=read(config.basin_table),
        existing_weights=read(config.weight_table) if has_weights else None,
        snapshots=snapshots,
    )
