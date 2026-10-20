import json

from flashflood_data.shared.spark.models import (
    PinnedSnapshot,
    PublishedSnapshot,
    SparkApplicationRequest,
)


def _request(arguments):
    return SparkApplicationRequest(
        application_id="silver-basin-app",
        pipeline_run_id="silver-basin-run",
        pipeline="silver_basin_topology",
        entrypoint="flashflood_data.pipelines.silver_basin.spark_job",
        snapshots=(PinnedSnapshot("bronze.basin_polygon_raw", 17),),
        build_signature="a" * 64,
        output_manifest_uri="s3://staging/silver/silver_basin_topology/run/result.json",
        arguments=arguments,
    )


def test_parse_job_arguments_requires_exact_selected_objects_and_outputs() -> None:
    import pytest

    from flashflood_data.pipelines.silver_basin.spark_job import parse_job_arguments

    parsed = parse_job_arguments(
        _request(
            (
                "hydro_object_id=hydro-1",
                "atlas_object_id=atlas-1",
                "basin_ids=101,102",
                "outputs=silver.basin_edge",
                "orchestrator_run_id=manual-1",
            )
        )
    )
    assert parsed.outputs == ("silver.basin_edge",)
    assert parsed.hydro_object_id == "hydro-1"
    assert parsed.basin_ids == ("101", "102")

    with pytest.raises(ValueError, match="arguments"):
        parse_job_arguments(_request(("hydro_object_id=hydro-1",)))


def test_manifest_payload_serializes_quality_timestamps_and_snapshots() -> None:
    from datetime import UTC, datetime

    from flashflood_data.pipelines.silver_basin.spark_job import manifest_payload

    payload = manifest_payload(
        _request(
            (
                "hydro_object_id=hydro-1",
                "atlas_object_id=atlas-1",
                "basin_ids=101,102",
                "outputs=silver.dim_basin",
                "orchestrator_run_id=manual-1",
            )
        ),
        outputs=(PublishedSnapshot("silver.dim_basin", 91, 2, 1),),
        quality_rows=(
            {
                "rule_id": "basin_geometry_valid",
                "checked_at": datetime(2026, 1, 1, tzinfo=UTC),
            },
        ),
    )
    decoded = json.loads(payload)

    assert decoded["build_signature"] == "a" * 64
    assert decoded["outputs"][0]["snapshot_id"] == 91
    assert decoded["quality_rows"][0]["checked_at"] == "2026-01-01T00:00:00+00:00"
