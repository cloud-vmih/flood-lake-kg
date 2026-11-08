"""Source-specific Bronze to Silver weather normalization contract."""

from dataclasses import dataclass
from pathlib import Path

from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config
from flashflood_data.pipelines.silver_weather.normalize import normalize_weather_slices

CONFIG = load_weather_aggregation_config(
    Path(__file__).resolve().parents[4] / "config/pipelines/silver/weather_aggregation.yaml"
)


@dataclass
class _Frame:
    views: list[str]

    def createOrReplaceTempView(self, name: str) -> None:
        self.views.append(name)


class _Catalog:
    def __init__(self) -> None:
        self.dropped: list[str] = []

    def dropTempView(self, name: str) -> None:
        self.dropped.append(name)


class _Spark:
    def __init__(self) -> None:
        self.views: list[str] = []
        self.catalog = _Catalog()
        self.query = ""

    def sql(self, query: str) -> str:
        self.query = query
        return "lazy-normalized-slices"


def test_gsmap_rate_converts_by_actual_window_duration() -> None:
    spark = _Spark()

    result = normalize_weather_slices(spark, _Frame(spark.views), CONFIG)

    assert result == "lazy-normalized-slices"
    assert "c.source_id = 'gsmap'" in spark.query
    assert "c.unit = 'mm/h'" in spark.query
    assert "c.value_kind = 'rate'" in spark.query
    assert "unix_micros(c.window_end) - unix_micros(c.window_start)" in spark.query
    assert "AS values" in spark.query
    assert spark.catalog.dropped == spark.views


def test_era5_deaccumulates_only_with_exact_previous_hour_and_aligned_cells() -> None:
    spark = _Spark()
    normalize_weather_slices(spark, _Frame(spark.views), CONFIG)

    assert "p.valid_time = c.valid_time - INTERVAL 1 HOUR" in spark.query
    assert "p.source_revision = c.source_revision" in spark.query
    assert "p.source_cycle_id <> c.source_cycle_id" in spark.query
    assert "dayofmonth(c.valid_time) = 1 AND hour(c.valid_time) = 0" in spark.query
    assert "p.spatial_scope_id = c.spatial_scope_id" in spark.query
    assert "c.cell_indices = p.previous_indices" in spark.query
    assert "hour(c.valid_time) = 1" in spark.query
    assert "zip_with(c.values, p.previous_values" in spark.query
    assert "c.valid_time - INTERVAL 1 HOUR" in spark.query
    assert "CAST(NULL AS DOUBLE)" in spark.query
    assert "ROW_NUMBER() OVER" in spark.query
    assert "GREATEST(c.available_at, p.previous_available_at)" in spark.query
    assert "normalization_previous_slice_id" in spark.query


def test_ifs_runoff_and_soil_layers_are_preserved_without_false_split() -> None:
    spark = _Spark()
    normalize_weather_slices(spark, _Frame(spark.views), CONFIG)

    assert "c.variable IN ('precipitation', 'runoff')" in spark.query
    assert "ELSE c.variable END AS variable" in spark.query
    assert "soil_moisture_0_to_7cm" in spark.query
    assert "volumetric_soil_water_layer_4" in spark.query
    assert "'subsurface_runoff'" in spark.query
    assert "'m3/m3'" in spark.query
    assert "'m³/m³'" in spark.query
    assert "'interval_sum'" in spark.query
    assert "'instantaneous'" in spark.query
    assert "assert_true(" in spark.query


def test_normalize_releases_temp_view_on_query_error() -> None:
    class BrokenSpark(_Spark):
        def sql(self, query: str) -> str:
            raise RuntimeError("analysis failed")

    spark = BrokenSpark()
    try:
        normalize_weather_slices(spark, _Frame(spark.views), CONFIG)
    except RuntimeError as error:
        assert str(error) == "analysis failed"
    else:
        raise AssertionError("expected Spark SQL failure")
    assert spark.catalog.dropped == spark.views


def test_batch_filters_current_rows_but_keeps_previous_month_predecessor() -> None:
    spark = _Spark()
    normalize_weather_slices(spark, _Frame(spark.views), CONFIG, batch_month="2020-02")

    assert "current_rows AS" in spark.query
    assert "s.valid_time >= TIMESTAMP '2020-02-01 00:00:00'" in spark.query
    assert "s.valid_time < TIMESTAMP '2020-03-01 00:00:00'" in spark.query
    assert "s.valid_time >= TIMESTAMP '2020-01-31 23:00:00'" in spark.query
    assert "FROM current_rows c" in spark.query
    assert "LEFT JOIN source_rows p" in spark.query
