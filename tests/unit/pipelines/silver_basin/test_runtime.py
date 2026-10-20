from flashflood_data.pipelines.silver_basin.planner import BasinExecutionPlan
from flashflood_data.shared.spark.models import PinnedSnapshot


def test_bronze_revision_discovery_keeps_one_pair_per_source_object() -> None:
    import pyarrow as pa

    from flashflood_data.pipelines.silver_basin.runtime import _distinct_bronze_objects

    table = pa.table(
        {
            "object_id": ["hydro-old", "hydro-old", "atlas-new", "hydro-new"],
            "source_id": ["hydro", "hydro", "atlas", "hydro"],
        }
    )
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=2))

    assert _distinct_bronze_objects(reader) == (
        {"source_id": "atlas", "object_id": "atlas-new"},
        {"source_id": "hydro", "object_id": "hydro-new"},
        {"source_id": "hydro", "object_id": "hydro-old"},
    )


def _plan(action="build", missing=("silver.dim_basin", "silver.basin_edge")):
    return BasinExecutionPlan(
        action=action,
        build_signature="a" * 64,
        pipeline_run_id="silver-basin-aaaaaaaaaaaaaaaaaaaa",
        snapshot=PinnedSnapshot("bronze.basin_polygon_raw", 17),
        hydro_object_id="hydro-object",
        atlas_object_id="atlas-object",
        basin_ids=("101", "102"),
        completed_outputs=(),
        missing_outputs=missing,
    )


def test_application_request_pins_revision_and_repair_outputs() -> None:
    from flashflood_data.pipelines.silver_basin.runtime import basin_application_request

    request = basin_application_request(
        _plan(action="repair", missing=("silver.basin_edge",)),
        orchestrator_run_id="manual__2026-10-18T01:00:00+00:00",
    )

    assert request.entrypoint == "flashflood_data.pipelines.silver_basin.spark_job"
    assert request.snapshots == (PinnedSnapshot("bronze.basin_polygon_raw", 17),)
    assert "hydro_object_id=hydro-object" in request.arguments
    assert "atlas_object_id=atlas-object" in request.arguments
    assert "basin_ids=101,102" in request.arguments
    assert "outputs=silver.basin_edge" in request.arguments
    assert request.output_manifest_uri.endswith("/result.json")


def test_runtime_skip_does_not_submit_application() -> None:
    from flashflood_data.pipelines.silver_basin.runtime import BasinSilverRuntime

    class Submitter:
        def submit(self, _request):
            raise AssertionError("skip must not submit Spark")

    runtime = BasinSilverRuntime(
        catalog=object(),
        object_store=object(),
        submitter=Submitter(),
        config=object(),
        policy=object(),
        config_hash="config-hash",
    )

    result = runtime.execute(
        _plan(action="skip", missing=()),
        orchestrator_run_id="manual-1",
    )
    assert result == {
        "status": "skipped",
        "pipeline_run_id": "silver-basin-aaaaaaaaaaaaaaaaaaaa",
        "build_signature": "a" * 64,
        "prior_run_id": None,
    }


def test_manifest_identity_rejects_mismatched_application() -> None:
    from flashflood_data.pipelines.silver_basin.runtime import validate_manifest
    from flashflood_data.shared.spark.models import SparkApplicationResult

    plan = _plan()
    result = SparkApplicationResult(
        application_id="basin-run-abc123",
        status="succeeded",
        output_manifest_uri="s3://staging/result.json",
    )
    manifest = {
        "application_id": "different-app",
        "pipeline_run_id": plan.pipeline_run_id,
        "build_signature": plan.build_signature,
        "outputs": [],
        "quality_rows": [],
    }

    try:
        validate_manifest(manifest, plan=plan, result=result)
    except ValueError as error:
        assert "identity" in str(error)
    else:
        raise AssertionError("mismatched Spark manifest identity must be rejected")
