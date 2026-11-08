"""Spark weather aggregation plan preserves cell alignment and coverage rules."""

from dataclasses import dataclass
from pathlib import Path

from flashflood_data.pipelines.silver_weather.aggregate import aggregate_weather_slices
from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config

CONFIG = load_weather_aggregation_config(
    Path(__file__).resolve().parents[4] / "config/pipelines/silver/weather_aggregation.yaml"
)


@dataclass
class _Frame:
    label: str
    views: dict[str, str]

    def createOrReplaceTempView(self, name: str) -> None:
        self.views[name] = self.label


class _Catalog:
    def __init__(self) -> None:
        self.dropped: list[str] = []

    def dropTempView(self, name: str) -> None:
        self.dropped.append(name)


class _Spark:
    def __init__(self) -> None:
        self.views: dict[str, str] = {}
        self.catalog = _Catalog()
        self.query = ""

    def sql(self, query: str) -> str:
        self.query = query
        assert set(self.views.values()) == {"weather", "grid", "weights"}
        return "lazy-basin-weather-frame"


def test_aggregate_aligns_arrays_and_renormalizes_by_valid_area() -> None:
    spark = _Spark()

    result = aggregate_weather_slices(
        spark,
        _Frame("weather", spark.views),
        _Frame("grid", spark.views),
        _Frame("weights", spark.views),
        CONFIG,
    )

    assert result == "lazy-basin-weather-frame"
    assert "posexplode(s.cell_indices)" in spark.query
    assert "element_at(s.values, cell_pos + 1)" in spark.query
    assert "size(s.cell_indices) = size(s.values)" in spark.query
    assert "size(array_distinct(s.cell_indices)) = size(s.cell_indices)" in spark.query
    assert "g.cell_index = e.cell_index" in spark.query
    assert "g.source_id = e.source_id" in spark.query
    assert "g.source_grid_version = e.source_grid_version" in spark.query
    assert "w.source_grid_id = c.source_grid_id" in spark.query
    assert "w.geometry_processing_version = 'equal-area-overlay-v1'" in spark.query
    assert "SUM(CASE WHEN cell_value IS NOT NULL" in spark.query
    assert "ABS(cell_value) < CAST('Infinity' AS DOUBLE)" in spark.query
    assert "weighted_sum / valid_weight" in spark.query
    assert "LEAST(1.0, valid_weight) AS valid_coverage_fraction" in spark.query
    assert "0.8" in spark.query
    assert "low_coverage" in spark.query
    assert "business_key_hash" in spark.query
    assert len(spark.catalog.dropped) == 3


def test_aggregate_requires_canonical_units_and_non_null_availability() -> None:
    spark = _Spark()
    aggregate_weather_slices(
        spark,
        _Frame("weather", spark.views),
        _Frame("grid", spark.views),
        _Frame("weights", spark.views),
        CONFIG,
    )

    assert "s.available_at IS NOT NULL" in spark.query
    assert "s.unit =" in spark.query
    assert "s.value_kind =" in spark.query
    assert "s.value_kind = 'instantaneous' AND s.window_end = s.window_start" in spark.query
    assert "assert_true(" in spark.query


def test_aggregate_releases_views_when_spark_query_fails() -> None:
    class BrokenSpark(_Spark):
        def sql(self, query: str) -> str:
            raise RuntimeError("analysis failed")

    spark = BrokenSpark()
    try:
        aggregate_weather_slices(
            spark,
            _Frame("weather", spark.views),
            _Frame("grid", spark.views),
            _Frame("weights", spark.views),
            CONFIG,
        )
    except RuntimeError as error:
        assert str(error) == "analysis failed"
    else:
        raise AssertionError("expected Spark SQL failure")
    assert len(spark.catalog.dropped) == 3
