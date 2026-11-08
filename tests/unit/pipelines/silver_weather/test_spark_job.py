"""Weather Spark job validates its pinned input and requested output set."""

import pytest

from flashflood_data.pipelines.silver_weather.spark_job import parse_job_arguments
from flashflood_data.shared.spark.models import PinnedSnapshot, SparkApplicationRequest


def _request(arguments=("outputs=silver.grid_basin_weight,silver.basin_weather_value",)):
    return SparkApplicationRequest(
        application_id="weather-test", pipeline_run_id="run-1",
        pipeline="silver_weather_aggregation",
        entrypoint="flashflood_data.pipelines.silver_weather.spark_job",
        snapshots=(
            PinnedSnapshot("bronze.weather_raster_slice", 1),
            PinnedSnapshot("silver.source_grid", 2),
            PinnedSnapshot("silver.dim_basin", 3),
        ),
        build_signature="a" * 64,
        output_manifest_uri="s3://staging/silver/weather/run-1/result.json",
        arguments=arguments,
    )


def test_spark_job_accepts_only_declared_weather_outputs() -> None:
    assert parse_job_arguments(_request(("outputs=silver.grid_basin_weight",))) == (
        ("silver.grid_basin_weight",), None
    )
    assert parse_job_arguments(_request((
        "outputs=silver.basin_weather_value", "batch_month=2020-01",
    ))) == (("silver.basin_weather_value",), "2020-01")
    with pytest.raises(ValueError, match="batch_month"):
        parse_job_arguments(_request())
    with pytest.raises(ValueError, match="batch_month"):
        parse_job_arguments(_request((
            "outputs=silver.basin_weather_value", "batch_month=2020-13",
        )))
    with pytest.raises(ValueError, match="outputs"):
        parse_job_arguments(_request(("outputs=silver.dim_basin",)))
    with pytest.raises(ValueError, match="outputs"):
        parse_job_arguments(_request(("outputs=silver.grid_basin_weight,silver.grid_basin_weight",)))


def test_weight_publish_detaches_source_from_iceberg_target(monkeypatch) -> None:
    from flashflood_data.pipelines.silver_weather import spark_job

    class Frame:
        def __init__(self):
            self.detached = False

        def limit(self, count):
            assert count == 1
            return self

        def count(self):
            return 1

        def localCheckpoint(self, *, eager):
            assert eager is True
            detached = Frame()
            detached.detached = True
            return detached

    published = []

    class Publisher:
        def __init__(self, spark, table):
            assert table == "flood_lakehouse.silver.grid_basin_weight"

        def publish(self, frame, *, key_fields, mode):
            published.append((frame.detached, key_fields, mode))
            return "snapshot"

    monkeypatch.setattr(spark_job, "_target_frame", lambda spark, table, frame: frame)
    monkeypatch.setattr(spark_job, "SparkTablePublisher", Publisher)

    output = spark_job._publish_or_reuse(
        object(), "flood_lakehouse.silver.grid_basin_weight", Frame(),
        spark_job.WEIGHT_KEY, detach_source=True,
    )

    assert output == "snapshot"
    assert published == [(True, spark_job.WEIGHT_KEY, "merge")]


def test_weather_normalization_is_materialized_before_aggregation(monkeypatch) -> None:
    from flashflood_data.pipelines.silver_weather import spark_job

    class Normalized:
        def checkpoint(self, *, eager):
            assert eager is True
            return "materialized-normalized"

    class Spark:
        class Context:
            def __init__(self):
                self.directory = None

            def setCheckpointDir(self, directory):
                self.directory = directory

        def __init__(self):
            self.sparkContext = self.Context()

    monkeypatch.setattr(
        spark_job, "normalize_weather_slices",
        lambda spark, weather, config, *, batch_month: Normalized(),
    )

    def aggregate(spark, normalized, grid, weights, config):
        assert normalized == "materialized-normalized"
        return "basin-values"

    monkeypatch.setattr(spark_job, "aggregate_weather_slices", aggregate)

    spark = Spark()
    assert spark_job._build_weather_values(
        spark, object(), object(), object(), object(), "/checkpoint/weather-run", "2020-01"
    ) == "basin-values"
    assert spark.sparkContext.directory == "/checkpoint/weather-run"


@pytest.mark.parametrize("deleted", [0, 2])
def test_weather_publish_finishes_replacement_before_cleanup_and_returns_latest_snapshot(
    monkeypatch, deleted,
) -> None:
    from flashflood_data.pipelines.silver_weather import spark_job

    events = []

    class Frame:
        def __init__(self, label):
            self.label = label

        def checkpoint(self, *, eager):
            assert eager is True
            events.append(f"{self.label}_materialized")
            return self

    class Spark:
        def table(self, table):
            return "existing"

    accepted, frame = Frame("accepted"), Frame("selected")

    def select(spark, candidate, existing, config, *, batch_month):
        assert existing == "existing" and batch_month == "2020-02"
        assert candidate is accepted
        events.append("selected")
        return frame

    def publish(spark, table, selected, keys, *, merge_scope):
        assert selected is frame
        assert keys == ("business_key_hash", "valid_time")
        assert merge_scope.column == "valid_time"
        assert merge_scope.start.isoformat() == "2020-02-01T00:00:00+00:00"
        assert merge_scope.end.isoformat() == "2020-03-01T00:00:00+00:00"
        events.append("published")
        return "snapshot-before-cleanup"

    def cleanup(spark, table, config, *, batch_month):
        events.append("cleaned")
        return deleted

    monkeypatch.setattr(spark_job, "prefer_gsmap", select)
    monkeypatch.setattr(spark_job, "_publish_or_reuse", publish)
    monkeypatch.setattr(spark_job, "remove_superseded_era5", cleanup)
    monkeypatch.setattr(spark_job, "_existing_evidence", lambda spark, table: "latest-snapshot")

    evidence = spark_job._publish_weather_values(
        Spark(), "catalog.silver.basin_weather_value", accepted, object(), "2020-02",
    )

    assert events == [
        "accepted_materialized", "selected", "selected_materialized", "published", "cleaned",
    ]
    assert evidence == ("latest-snapshot" if deleted else "snapshot-before-cleanup")
