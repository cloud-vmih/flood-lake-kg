from __future__ import annotations

import json
from pathlib import Path

import pytest

from flashflood_data.shared.spark.models import (
    BuildSignature,
    PinnedSnapshot,
    SparkApplicationRequest,
    load_spark_runtime_config,
)

ROOT = Path(__file__).parents[4]


def test_local_runtime_config_pins_the_supported_matrix_and_bounded_resources() -> None:
    config = load_spark_runtime_config(ROOT / "config/spark/local.yaml")

    assert (config.spark_version, config.scala_version, config.java_version) == (
        "4.1.3",
        "2.13",
        "17",
    )
    assert config.iceberg_version == "1.11.0"
    assert config.sedona_version == "1.9.1"
    assert config.driver_memory == "1g"
    assert config.executor_memory == "2g"
    assert config.executor_cores == 2
    assert config.application_timeout_seconds == 3600
    assert config.endpoint_env == {
        "catalog_uri": "POLARIS_URI",
        "minio_endpoint": "MINIO_ENDPOINT",
    }


def test_runtime_config_rejects_an_unsupported_version_matrix(tmp_path: Path) -> None:
    source = (ROOT / "config/spark/local.yaml").read_text(encoding="utf-8")
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text(source.replace('spark: "4.1.3"', 'spark: "3.5.0"'), encoding="utf-8")

    with pytest.raises(ValueError, match="unsupported Spark runtime matrix"):
        load_spark_runtime_config(invalid)


def test_application_request_serialization_contains_references_but_no_credentials() -> None:
    snapshots = (
        PinnedSnapshot("bronze.basin_polygon_raw", 17),
        PinnedSnapshot("meta.dataset_registry", 9),
    )
    signature = BuildSignature.create(
        snapshots=snapshots,
        config_hash="config-v1",
        policy_id="policy-v1",
        algorithm_version="topology-v1",
        contract_version="v1",
    )
    request = SparkApplicationRequest(
        application_id="silver-basin-topology-run-1",
        pipeline_run_id="run-1",
        pipeline="silver_basin_topology",
        entrypoint="flashflood_data.pipelines.silver_basin_topology.spark_job",
        snapshots=snapshots,
        build_signature=signature.value,
        output_manifest_uri="s3://staging/silver/basin_topology/run-1/result.json",
    )

    payload = request.to_json()

    assert json.loads(payload)["snapshots"][0]["snapshot_id"] == 17
    assert "secret" not in payload.lower()
    assert "credential" not in payload.lower()

