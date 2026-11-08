"""Weather Spark request and manifest identity stay small across Airflow."""

from flashflood_data.pipelines.silver_weather.pipeline import plan_weather_build
from flashflood_data.pipelines.silver_weather.runtime import weather_application_request
from flashflood_data.shared.spark.models import PinnedSnapshot


def test_request_pins_inputs_and_uses_weather_spark_entrypoint() -> None:
    plan = plan_weather_build(
        (
            PinnedSnapshot("bronze.weather_raster_slice", 1),
            PinnedSnapshot("silver.source_grid", 2),
            PinnedSnapshot("silver.dim_basin", 3),
        ),
        config_hash="b" * 64,
        history=(),
    )
    request = weather_application_request(
        plan, orchestrator_run_id="manual__1",
        outputs=("silver.basin_weather_value",), batch_month="2020-01",
    )
    assert request.snapshots == plan.snapshots
    assert request.entrypoint == "flashflood_data.pipelines.silver_weather.spark_job"
    assert request.output_manifest_uri.startswith("s3://staging/silver/silver_weather_aggregation/")
    assert request.arguments == ("outputs=silver.basin_weather_value", "batch_month=2020-01")
    assert "/batch=2020-01/" in request.output_manifest_uri
