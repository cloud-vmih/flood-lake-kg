"""Pinned input reads and the lazy Sedona grid-to-basin overlay."""

from dataclasses import dataclass
from pathlib import Path

from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config
from flashflood_data.pipelines.silver_weather.reader import read_weather_silver_inputs
from flashflood_data.pipelines.silver_weather.weights import build_grid_basin_weights
from flashflood_data.shared.spark.models import PinnedSnapshot

CONFIG = load_weather_aggregation_config(
    Path(__file__).resolve().parents[4] / "config/pipelines/silver/weather_aggregation.yaml"
)


class _Catalog:
    def __init__(self, has_weights: bool) -> None:
        self.has_weights = has_weights
        self.asked: list[str] = []

    def tableExists(self, table: str) -> bool:
        self.asked.append(table)
        return self.has_weights


class _Spark:
    def __init__(self, has_weights: bool = False) -> None:
        self.catalog = _Catalog(has_weights)


class _SnapshotManager:
    def __init__(self) -> None:
        self.pinned: list[tuple[str, ...]] = []
        self.read_ids: list[PinnedSnapshot] = []

    def qualify(self, table: str) -> str:
        return f"lake.{table}"

    def pin(self, spark: object, tables: tuple[str, ...]) -> tuple[PinnedSnapshot, ...]:
        self.pinned.append(tables)
        return tuple(PinnedSnapshot(table, index + 1) for index, table in enumerate(tables))

    def read(self, spark: object, snapshot: PinnedSnapshot) -> str:
        self.read_ids.append(snapshot)
        return f"{snapshot.table}@{snapshot.snapshot_id}"


def test_reader_pins_weather_grid_and_basin_when_weight_table_is_absent() -> None:
    spark = _Spark()
    manager = _SnapshotManager()

    inputs = read_weather_silver_inputs(spark, manager, CONFIG)

    assert manager.pinned == [(CONFIG.weather_table, CONFIG.grid_table, CONFIG.basin_table)]
    assert [item.table for item in inputs.snapshots] == list(manager.pinned[0])
    assert inputs.weather == "bronze.weather_raster_slice@1"
    assert inputs.grid == "silver.source_grid@2"
    assert inputs.basins == "silver.dim_basin@3"
    assert inputs.existing_weights is None
    assert manager.read_ids == list(inputs.snapshots)
    assert spark.catalog.asked == ["lake.silver.grid_basin_weight"]


def test_reader_pins_existing_weights_with_other_inputs() -> None:
    spark = _Spark(has_weights=True)
    manager = _SnapshotManager()

    inputs = read_weather_silver_inputs(spark, manager, CONFIG)

    assert manager.pinned == [
        (CONFIG.weather_table, CONFIG.grid_table, CONFIG.basin_table, CONFIG.weight_table)
    ]
    assert inputs.existing_weights == "silver.grid_basin_weight@4"
    assert manager.read_ids == list(inputs.snapshots)


def test_reader_treats_created_but_empty_weight_table_as_no_weights() -> None:
    class EmptyWeightManager(_SnapshotManager):
        def pin(self, spark: object, tables: tuple[str, ...]) -> tuple[PinnedSnapshot, ...]:
            if CONFIG.weight_table in tables:
                self.pinned.append(tables)
                raise ValueError(f"Iceberg input table has no snapshot: {CONFIG.weight_table}")
            return super().pin(spark, tables)

    manager = EmptyWeightManager()
    inputs = read_weather_silver_inputs(_Spark(has_weights=True), manager, CONFIG)

    assert inputs.existing_weights is None
    assert len(inputs.snapshots) == 3
    assert len(manager.pinned) == 2


@dataclass
class _Frame:
    name: str
    views: dict[str, str]

    def createOrReplaceTempView(self, name: str) -> None:
        self.views[name] = self.name


class _OverlayCatalog:
    def __init__(self) -> None:
        self.dropped: list[str] = []

    def dropTempView(self, name: str) -> None:
        self.dropped.append(name)


class _OverlaySpark:
    def __init__(self) -> None:
        self.views: dict[str, str] = {}
        self.catalog = _OverlayCatalog()
        self.query = ""

    def sql(self, query: str) -> str:
        self.query = query
        assert set(self.views.values()) == {"grid", "basin"}
        return "lazy-weight-frame"


def test_weight_overlay_uses_equal_area_intersection_and_versioned_keys() -> None:
    spark = _OverlaySpark()

    result = build_grid_basin_weights(
        spark, _Frame("grid", spark.views), _Frame("basin", spark.views), CONFIG
    )

    assert result == "lazy-weight-frame"
    assert "ST_Intersects" in spark.query
    assert "ST_Intersection" in spark.query
    assert "ST_Area" in spark.query
    assert "EPSG:6933" in spark.query
    assert "source_grid.crs, 'EPSG:6933'" in spark.query
    assert "source_basin.crs, 'EPSG:6933'" in spark.query
    assert "intersection_area_m2 / basin_area_m2" in spark.query
    assert "intersection_area_m2 / grid_area_m2" in spark.query
    assert "intersection_area_m2 > 0" in spark.query
    assert "source_grid_version" in spark.query
    assert "basin_version" in spark.query
    assert "geometry_processing_version" in spark.query
    assert len(spark.catalog.dropped) == 2


def test_weight_overlay_releases_views_if_query_fails() -> None:
    class BrokenSpark(_OverlaySpark):
        def sql(self, query: str) -> str:
            raise RuntimeError("Sedona unavailable")

    spark = BrokenSpark()
    try:
        build_grid_basin_weights(
            spark, _Frame("grid", spark.views), _Frame("basin", spark.views), CONFIG
        )
    except RuntimeError as error:
        assert str(error) == "Sedona unavailable"
    else:
        raise AssertionError("expected Spark SQL failure")
    assert len(spark.catalog.dropped) == 2
