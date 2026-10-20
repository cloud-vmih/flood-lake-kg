from __future__ import annotations

import json
import subprocess

from flashflood_data.shared.spark.models import (
    PinnedSnapshot,
    SparkApplicationRequest,
)


def _request() -> SparkApplicationRequest:
    return SparkApplicationRequest(
        application_id="basin-run-abc123",
        pipeline_run_id="run-1",
        pipeline="silver_basin_topology",
        entrypoint="flashflood_data.pipelines.silver_basin.spark_job",
        snapshots=(PinnedSnapshot("bronze.basin_polygon_raw", 17),),
        build_signature="abc123",
        output_manifest_uri="s3://staging/silver/basin/run-1/result.json",
    )


def test_submitter_parses_structured_result_and_builds_stable_command() -> None:
    from flashflood_data.shared.spark.submit import RESULT_PREFIX, SparkSubmitter

    request = _request()
    payload = {
        "application_id": request.application_id,
        "status": "succeeded",
        "output_manifest_uri": request.output_manifest_uri,
        "published_snapshots": [],
        "error_code": None,
    }
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout=RESULT_PREFIX + json.dumps(payload))

    submitter = SparkSubmitter(
        command_prefix=("spark-submit",),
        master="spark://spark-master:7077",
        cli_path="/opt/flashflood/src/flashflood_data/shared/spark/cli.py",
        timeout_seconds=60,
        runner=run,
    )

    first = submitter.submit(request)
    second = submitter.submit(request)

    assert first.status == second.status == "succeeded"
    assert calls[0][0] == calls[1][0]
    assert calls[0][0][-2] == "--request-json"
    assert json.loads(calls[0][0][-1])["application_id"] == request.application_id
    assert calls[0][1]["timeout"] == 60


def test_submitter_returns_timeout_without_changing_application_identity() -> None:
    from flashflood_data.shared.spark.submit import SparkSubmitter

    request = _request()

    def timeout(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    result = SparkSubmitter(runner=timeout, timeout_seconds=5).submit(request)

    assert result.application_id == request.application_id
    assert result.status == "timed_out"
    assert result.error_code == "spark_submit_timeout"


def test_submitter_preserves_structured_domain_failure_on_nonzero_exit() -> None:
    from flashflood_data.shared.spark.submit import RESULT_PREFIX, SparkSubmitter

    request = _request()
    payload = {
        "application_id": request.application_id,
        "status": "failed",
        "output_manifest_uri": None,
        "published_snapshots": [],
        "error_code": "basin_quality_blocked",
    }

    def failed(command, **kwargs):
        del kwargs
        return subprocess.CompletedProcess(
            command,
            1,
            stdout=RESULT_PREFIX + json.dumps(payload),
            stderr="domain failed",
        )

    result = SparkSubmitter(runner=failed).submit(request)
    assert result.status == "failed"
    assert result.error_code == "basin_quality_blocked"


def test_gateway_submitter_posts_request_and_validates_application_identity() -> None:
    from flashflood_data.shared.spark.submit import SparkGatewaySubmitter

    request = _request()
    payload = {
        "application_id": request.application_id,
        "status": "succeeded",
        "output_manifest_uri": request.output_manifest_uri,
        "published_snapshots": [],
        "error_code": None,
    }
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self):
            return json.dumps(payload).encode()

    def open_request(http_request, timeout):
        calls.append((http_request, timeout))
        return Response()

    result = SparkGatewaySubmitter(
        "http://spark-submit-gateway:8090", timeout_seconds=60, opener=open_request
    ).submit(request)

    assert result.status == "succeeded"
    assert calls[0][0].full_url.endswith("/submit")
    assert json.loads(calls[0][0].data) == json.loads(request.to_json())
    assert calls[0][1] == 60
