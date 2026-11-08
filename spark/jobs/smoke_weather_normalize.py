"""Small Spark fixture for GSMaP, ERA5-Land and IFS Silver normalization."""

from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config
from flashflood_data.pipelines.silver_weather.normalize import normalize_weather_slices

UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10.


def dt(hour: int, day: int = 2, month: int = 1):
    return datetime(2020, month, day, hour, tzinfo=UTC)


schema = StructType(
    [
        StructField("slice_id", StringType()),
        StructField("object_id", StringType()),
        StructField("source_id", StringType()),
        StructField("source_product", StringType()),
        StructField("source_grid_version", StringType()),
        StructField("spatial_scope_id", StringType()),
        StructField("variable", StringType()),
        StructField("vertical_level", StringType()),
        StructField("source_cycle_id", StringType()),
        StructField("model_run_time", TimestampType()),
        StructField("valid_time", TimestampType()),
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("source_revision", IntegerType()),
        StructField("cell_indices", ArrayType(LongType())),
        StructField("values", ArrayType(DoubleType())),
        StructField("unit", StringType()),
        StructField("value_kind", StringType()),
        StructField("available_at", TimestampType()),
        StructField("ingest_run_id", StringType()),
        StructField("parser_version", StringType()),
        StructField("quality_status", StringType()),
    ]
)


def row(
    slice_id, source_id, variable, unit, kind, valid, start, values, *,
    cycle="a", available=None, revision=0,
):
    return (
        slice_id,
        "obj",
        source_id,
        "product",
        "grid-v1",
        "scope-v1",
        variable,
        "surface",
        cycle,
        None,
        valid,
        start,
        valid,
        revision,
        [1, 2],
        values,
        unit,
        kind,
        available or dt(6),
        "run",
        "parser",
        "passed",
    )


spark = (
    SparkSession.builder.master("local[1]")
    .appName("weather-normalize-smoke")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
try:
    rows = [
        row("gsmap", "gsmap", "precipitation", "mm/h", "rate", dt(1), dt(0), [2.0, None]),
        row(
            "era23",
            "era5_land",
            "total_precipitation",
            "m",
            "accumulation_since_00_utc",
            dt(23, 1),
            dt(0, 1),
            [0.008, 0.009],
            cycle="prev",
            available=dt(8),
        ),
        row(
            "era00",
            "era5_land",
            "total_precipitation",
            "m",
            "accumulation_since_00_utc",
            dt(0),
            dt(0, 1),
            [0.009, 0.011],
        ),
        row(
            "era01",
            "era5_land",
            "total_precipitation",
            "m",
            "accumulation_since_00_utc",
            dt(1),
            dt(0),
            [0.002, 0.003],
        ),
        row(
            "era02",
            "era5_land",
            "total_precipitation",
            "m",
            "accumulation_since_00_utc",
            dt(2),
            dt(0),
            [0.005, 0.005],
        ),
        row(
            "era04",
            "era5_land",
            "total_precipitation",
            "m",
            "accumulation_since_00_utc",
            dt(4),
            dt(0),
            [0.009, 0.01],
        ),
        row(
            "ifs-soil",
            "ifs_openmeteo",
            "soil_moisture_0_to_7cm",
            "m³/m³",
            "instantaneous",
            dt(1),
            dt(1),
            [0.2, 0.3],
        ),
        row(
            "ifs-runoff",
            "ifs_openmeteo",
            "runoff",
            "mm",
            "preceding_hour_sum",
            dt(1),
            dt(0),
            [1.0, 2.0],
        ),
    ]
    config = load_weather_aggregation_config(
        Path("/opt/flashflood/config/pipelines/silver/weather_aggregation.yaml")
    )
    result = normalize_weather_slices(spark, spark.createDataFrame(rows, schema), config)
    values = {item.slice_id: item for item in result.collect()}
    assert values["gsmap"].values == [2.0, None]
    assert abs(values["era00"].values[0] - 1.0) < 1e-6
    assert abs(values["era01"].values[0] - 2.0) < 1e-6
    assert abs(values["era02"].values[0] - 3.0) < 1e-6
    assert values["era04"].values == [None, None]
    assert values["era00"].normalization_previous_slice_id == "era23"
    assert values["era00"].available_at == dt(8).replace(tzinfo=None)
    assert values["ifs-soil"].variable == "soil_moisture"
    assert values["ifs-soil"].unit == "m3/m3"
    assert values["ifs-soil"].window_start == values["ifs-soil"].window_end
    assert values["ifs-runoff"].variable == "runoff"
    boundary_rows = [
        row(
            "month-prev", "era5_land", "total_precipitation", "m",
            "accumulation_since_00_utc", dt(23, 31), dt(0, 31), [0.008, 0.009],
            cycle="20200101T0000Z", revision=101,
        ),
        row(
            "month-first", "era5_land", "total_precipitation", "m",
            "accumulation_since_00_utc", dt(0, 1, 2), dt(0, 31), [0.009, 0.011],
            cycle="20200201T0000Z", revision=202,
        ),
        row(
            "month-reset", "era5_land", "total_precipitation", "m",
            "accumulation_since_00_utc", dt(1, 1, 2), dt(0, 1, 2), [0.002, 0.003],
            cycle="20200201T0000Z", revision=202,
        ),
        row(
            "same-cycle-revised", "era5_land", "total_precipitation", "m",
            "accumulation_since_00_utc", dt(2, 1, 2), dt(0, 1, 2), [0.005, 0.005],
            cycle="20200201T0000Z", revision=303,
        ),
    ]
    boundary = normalize_weather_slices(
        spark, spark.createDataFrame(boundary_rows, schema), config, batch_month="2020-02",
    )
    by_id = {item.slice_id: item for item in boundary.collect()}
    assert "month-prev" not in by_id
    assert by_id["month-first"].normalization_previous_slice_id == "month-prev"
    assert all(
        value is not None and abs(value - expected) < 1e-6
        for value, expected in zip(by_id["month-first"].values, (1.0, 2.0), strict=True)
    )
    assert by_id["same-cycle-revised"].values == [None, None]
    print("WEATHER_NORMALIZE_SMOKE_OK")
finally:
    spark.stop()
