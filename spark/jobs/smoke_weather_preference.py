"""Exercise GSMaP preference and late-arrival cleanup on a disposable Iceberg table."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from flashflood_data.pipelines.silver_weather.aggregate import aggregate_weather_slices
from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config
from flashflood_data.pipelines.silver_weather.preference import (
    prefer_gsmap,
    remove_superseded_era5,
)
from flashflood_data.pipelines.silver_weather.quality import weather_quality_plan
from flashflood_data.pipelines.silver_weather.spark_job import WEATHER_KEY, _publish_weather_values
from flashflood_data.shared.spark.publish import SparkTablePublisher, TimestampMergeScope
from flashflood_data.shared.spark.quality import apply_quality_plan

UTC = timezone.utc  # noqa: UP017 - Spark image uses Python 3.10.
TABLE = "weather_test.silver.basin_weather_value"
SCHEMA = (
    "business_key_hash string, basin_id string, basin_version string, source_id string, "
    "source_product string, valid_time timestamp, window_start timestamp, "
    "window_end timestamp, variable string, vertical_level string, value double, "
    "unit string, value_kind string, valid_coverage_fraction double"
)


def row(key, source, *, basin="a", version="v1", minute=0, value=2.0,
        coverage=1.0, variable="precipitation", product=None):
    end = datetime(2020, 2, 1, 1, minute, tzinfo=UTC)
    return (key, basin, version, source, product or source, end,
            end - timedelta(hours=1), end, variable, "surface", value,
            "mm", "interval_sum", coverage)


config = load_weather_aggregation_config(
    Path("/opt/flashflood/config/pipelines/silver/weather_aggregation.yaml")
)
with TemporaryDirectory(prefix="weather-preference-smoke-") as workspace:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("weather-preference-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.weather_test", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.weather_test.type", "hadoop")
        .config("spark.sql.catalog.weather_test.warehouse", f"{workspace}/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(f"{workspace}/checkpoints")
    try:
        spark.sql("CREATE NAMESPACE weather_test.silver")
        spark.sql(f"CREATE TABLE {TABLE} ({SCHEMA}) USING iceberg")
        candidates = spark.createDataFrame([
            row("era-a", "era5_land"),
            row("gsmap-zero", "gsmap", value=0.0),
            row("era-low", "era5_land", basin="low"),
            row("gsmap-low", "gsmap", basin="low", coverage=0.4),
            row("era-null", "era5_land", basin="null"),
            row("gsmap-null", "gsmap", basin="null", value=None),
            row("era-negative", "era5_land", basin="negative"),
            row("gsmap-negative", "gsmap", basin="negative", value=-1.0),
            row("era-nan", "era5_land", basin="nan"),
            row("gsmap-nan", "gsmap", basin="nan", value=float("nan")),
            row("era-inf", "era5_land", basin="inf"),
            row("gsmap-inf", "gsmap", basin="inf", value=float("inf")),
            row("era-shift", "era5_land", basin="shift"),
            row("now-shift", "gsmap", basin="shift", minute=30, product="gauge_now_v8"),
            row("gsmap-now", "gsmap", product="gauge_now_v8"),
            row("era-old-version", "era5_land", version="old"),
            row("era-runoff", "era5_land", variable="surface_runoff"),
            row("ifs", "ifs_openmeteo"),
        ], SCHEMA)
        selected = prefer_gsmap(
            spark, candidates, spark.table(TABLE), config, batch_month="2020-02",
        )
        keys = {item.business_key_hash for item in selected.select("business_key_hash").collect()}
        assert keys == {item[0] for item in candidates.collect()} - {"era-a"}, keys

        # Publish first, then remove superseded fallback rows. A retry between
        # these two commits must still finish the cleanup, without duplicate keys.
        original = spark.createDataFrame([
            row("old-fallback", "era5_land", basin="late"),
            row("old-fallback-shift", "era5_land", basin="late", minute=30),
            row("old-runoff", "era5_land", basin="late", variable="surface_runoff"),
        ], SCHEMA)
        SparkTablePublisher(spark, TABLE).publish(
            original, key_fields=("business_key_hash",), mode="merge",
        )
        late = spark.createDataFrame([
            row("late-standard", "gsmap", basin="late", product="gauge_standard_v8"),
        ], SCHEMA)
        SparkTablePublisher(spark, TABLE).publish(
            late, key_fields=("business_key_hash",), mode="merge",
        )
        assert remove_superseded_era5(spark, TABLE, config, batch_month="2020-02") == 1
        remaining = {item.business_key_hash for item in spark.table(TABLE).collect()}
        assert remaining == {"old-fallback-shift", "old-runoff", "late-standard"}, remaining
        snapshot = spark.sql(f"SELECT COUNT(*) n FROM {TABLE}.snapshots").first().n
        assert remove_superseded_era5(spark, TABLE, config, batch_month="2020-02") == 0
        assert spark.sql(f"SELECT COUNT(*) n FROM {TABLE}.snapshots").first().n == snapshot

        # Existing valid GSMaP also suppresses a newly replayed ERA5 fallback.
        replay = spark.createDataFrame([row("replayed-era", "era5_land", basin="late")], SCHEMA)
        assert prefer_gsmap(
            spark, replay, spark.table(TABLE), config, batch_month="2020-02",
        ).count() == 0
        # Cleaning another month must leave the current month's fallback alone.
        shifted = spark.createDataFrame([
            row("late-now-shifted", "gsmap", basin="late", minute=30, product="gauge_now_v8"),
        ], SCHEMA)
        SparkTablePublisher(spark, TABLE).publish(
            shifted, key_fields=("business_key_hash",), mode="merge",
        )
        assert remove_superseded_era5(spark, TABLE, config, batch_month="2020-01") == 0
        assert "old-fallback-shift" in {
            item.business_key_hash for item in spark.table(TABLE).collect()
        }

        # Exercise the real aggregation/DQ/publish path. Filtering its lazy
        # unit-assertion plan directly can fail even though DQ passed; the
        # publisher must materialize accepted basin rows before preference.
        normalized_schema = (
            "slice_id string, source_id string, source_product string, source_grid_version string, "
            "source_cycle_id string, model_run_time timestamp, valid_time timestamp, "
            "window_start timestamp, window_end timestamp, variable string, vertical_level string, "
            "source_revision int, available_at timestamp, cell_indices array<bigint>, "
            "values array<double>, unit string, value_kind string"
        )
        end = datetime(2020, 2, 2, 1, tzinfo=UTC)
        normalized = spark.createDataFrame([
            (key, source, "product", "v1", "cycle", None, end, end - timedelta(hours=1),
             end, variable, "layer_1" if variable == "soil_moisture" else "surface",
             0, end, [1], [value], unit, kind)
            for key, source, variable, value, unit, kind in (
                ("gsmap-lazy", "gsmap", "precipitation", 0.0, "mm", "interval_sum"),
                ("era-rain-lazy", "era5_land", "precipitation", 2.0, "mm", "interval_sum"),
                ("era-soil-lazy", "era5_land", "soil_moisture", 0.3, "m3/m3", "instantaneous"),
            )
        ], normalized_schema)
        grid = spark.createDataFrame([
            (1, source, "v1", source + "-cell") for source in ("gsmap", "era5_land")
        ], "cell_index bigint, source_id string, source_grid_version string, source_grid_id string")
        weights = spark.createDataFrame([
            (source, "v1", source + "-cell", "lazy", "v1", config.geometry_processing_version, 1.0)
            for source in ("gsmap", "era5_land")
        ], "source_id string, source_grid_version string, source_grid_id string, "
           "basin_id string, basin_version string, geometry_processing_version string, "
           "weight_by_basin double")
        values = aggregate_weather_slices(spark, normalized, grid, weights, config)
        checked = apply_quality_plan(
            values, weather_quality_plan(config), pipeline_run_id="smoke-preference",
            dataset_id=config.output_table, key_fields=WEATHER_KEY,
        )
        assert not checked.blocked
        _publish_weather_values(spark, TABLE, checked.accepted, config, "2020-02")
        observed = {
            (item.source_id, item.variable, item.value)
            for item in spark.table(TABLE).filter("basin_id = 'lazy'").collect()
        }
        assert observed == {("gsmap", "precipitation", 0.0), ("era5_land", "soil_moisture", 0.3)}

        # A pending update replaces the same key's previous valid GSMaP value.
        # The old row must not suppress ERA5 if the replacement is now null or
        # low coverage. This occurs when weights or coverage policy change.
        previous_gsmap = spark.createDataFrame([
            row("recomputed-gsmap", "gsmap", basin="changed"),
        ], SCHEMA)
        SparkTablePublisher(spark, TABLE).publish(
            previous_gsmap, key_fields=("business_key_hash",), mode="merge",
        )
        recomputed = spark.createDataFrame([
            row("recomputed-gsmap", "gsmap", basin="changed", value=None, coverage=0.4),
            row("era-changed", "era5_land", basin="changed"),
        ], SCHEMA)
        preferred = prefer_gsmap(
            spark, recomputed, spark.table(TABLE), config, batch_month="2020-02",
        ).checkpoint(eager=True)
        assert "era-changed" in {item.business_key_hash for item in preferred.collect()}
        SparkTablePublisher(spark, TABLE).publish(
            preferred, key_fields=("business_key_hash",), mode="merge",
        )
        assert remove_superseded_era5(spark, TABLE, config, batch_month="2020-02") == 0
        assert prefer_gsmap(
            spark, recomputed, spark.table(TABLE), config, batch_month="2020-02",
        ).count() == 2

        # Bounded MERGE still preserves other months and refuses source rows
        # outside its scope, rather than silently inserting duplicate keys.
        january = list(row("january-control", "era5_land", basin="january"))
        for index in (5, 6, 7):
            january[index] -= timedelta(days=31)
        january_frame = spark.createDataFrame([tuple(january)], SCHEMA)
        SparkTablePublisher(spark, TABLE).publish(
            january_frame, key_fields=("business_key_hash",), mode="merge",
        )
        scope = TimestampMergeScope(
            "valid_time", datetime(2020, 2, 1, tzinfo=UTC), datetime(2020, 3, 1, tzinfo=UTC),
        )
        SparkTablePublisher(spark, TABLE).publish(
            recomputed, key_fields=("business_key_hash", "valid_time"),
            mode="merge", merge_scope=scope,
        )
        assert spark.table(TABLE).filter("business_key_hash = 'january-control'").count() == 1
        before = spark.sql(f"SELECT COUNT(*) n FROM {TABLE}.snapshots").first().n
        try:
            SparkTablePublisher(spark, TABLE).publish(
                january_frame, key_fields=("business_key_hash", "valid_time"),
                mode="merge", merge_scope=scope,
            )
        except ValueError as error:
            assert "outside merge scope" in str(error)
        else:
            raise AssertionError("out-of-scope source must be rejected")
        assert spark.sql(f"SELECT COUNT(*) n FROM {TABLE}.snapshots").first().n == before
        print("WEATHER_PREFERENCE_SMOKE_OK", flush=True)
    finally:
        spark.stop()
