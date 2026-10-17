#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
DOCKER_BIN=${DOCKER_BIN:-docker}

cd "$PROJECT_ROOT"
printf '%s\n' 'DQ quarantine smoke: running.'

"$DOCKER_BIN" compose exec -T airflow-scheduler python - <<'PY'
from datetime import UTC, datetime
from uuid import uuid4

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.shared.metadata.service import MetaRecorder
from flashflood_data.shared.quality import (
    DqPublishCoordinator,
    DqPublishRequest,
    QualityEvaluation,
    QualityFinding,
    QuarantineRepository,
    RejectedRecord,
)
from flashflood_data.storage.iceberg import load_polaris_catalog
from flashflood_data.storage.iceberg_tables import IcebergTableStore

token = uuid4().hex
namespace = f"smoke_dq_{token}"
object_id = f"mixed-{token}"
target = f"{namespace}.admin_boundary_raw"
settings = LakehouseSettings(_env_file=None)
catalog = load_polaris_catalog(settings)
store = IcebergTableStore(catalog)
coordinator = DqPublishCoordinator(
    target_writer=store,
    quarantine=QuarantineRepository(store, namespace=namespace),
    meta=MetaRecorder(store, meta_namespace=namespace),
)
now = datetime.now(UTC)
warning = QualityFinding(
    rule_id="geometry_repaired",
    rule_version="v1",
    status="failed",
    severity="warning",
    scope_key="valid-1",
    observed_value={"repaired": True},
    expected_value={"repaired": False},
    failed_row_count=1,
)
reject = QualityFinding(
    rule_id="bbox_valid",
    rule_version="v1",
    status="failed",
    severity="reject",
    scope_key="invalid-1",
    observed_value={"bbox": None},
    expected_value={"finite": True},
    failed_row_count=1,
)
accepted = {
    "object_id": object_id,
    "source_feature_id": "valid-1",
    "source_id": "fixture",
    "source_fields_json": "{}",
    "geometry_wkb": b"fixture-wkb",
    "crs": "EPSG:4326",
    "bbox_wgs84": [103.0, 20.0, 104.0, 21.0],
    "ingest_run_id": token,
    "parser_version": "v1",
    "quality_status": "warning",
}
evaluation = QualityEvaluation.from_parts(
    accepted_rows=(accepted,),
    rejected_rows=(
        RejectedRecord(
            source_record_key="invalid-1",
            record={**accepted, "source_feature_id": "invalid-1", "bbox_wgs84": None},
            record_uri=None,
            findings=(reject,),
        ),
    ),
    findings=(warning, reject),
    input_row_count=2,
)
request = DqPublishRequest(
    pipeline_run_id=f"mixed-{token}",
    orchestrator_run_id=token,
    layer="bronze",
    target_dataset_id=target,
    object_id=object_id,
    source_id="fixture",
    policy_id="fixture:v1",
    parser_version="v1",
    started_at=now,
)

try:
    first = coordinator.publish_object(request, evaluation)
    second = coordinator.publish_object(request, evaluation)
    if first.status != "succeeded_with_warning" or second.output_snapshot_id != first.output_snapshot_id:
        raise RuntimeError("mixed publication or retry repair failed")
    target_rows = store.ensure_table((namespace, "admin_boundary_raw")).scan().to_arrow().to_pylist()
    rejected_rows = store.ensure_table((namespace, "rejected_rows")).scan().to_arrow().to_pylist()
    if len(target_rows) != 1 or len(rejected_rows) != 1:
        raise RuntimeError("DQ retry created duplicate target or quarantine rows")

    fatal = QualityFinding(
        rule_id="object_readable",
        rule_version="v1",
        status="failed",
        severity="fatal",
        scope_key="fatal-object",
        observed_value={"readable": False},
        expected_value={"readable": True},
        failed_row_count=1,
    )
    fatal_evaluation = QualityEvaluation.from_parts(
        accepted_rows=(), rejected_rows=(), findings=(fatal,), input_row_count=0
    )
    fatal_request = DqPublishRequest(
        pipeline_run_id=f"fatal-{token}",
        orchestrator_run_id=token,
        layer="bronze",
        target_dataset_id=target,
        object_id=f"fatal-{token}",
        source_id="fixture",
        policy_id="fixture:v1",
        parser_version="v1",
    )
    fatal_result = coordinator.publish_object(fatal_request, fatal_evaluation)
    rejected_objects = (
        store.ensure_table((namespace, "rejected_objects")).scan().to_arrow().to_pylist()
    )
    target_rows = store.ensure_table((namespace, "admin_boundary_raw")).scan().to_arrow().to_pylist()
    if fatal_result.status != "failed_data_quality" or len(rejected_objects) != 1:
        raise RuntimeError("fatal object rejection evidence is incomplete")
    if any(row["object_id"] == fatal_request.object_id for row in target_rows):
        raise RuntimeError("fatal object was published")
finally:
    for table_name in (
        "admin_boundary_raw",
        "rejected_rows",
        "rejected_objects",
        "quality_results",
        "table_snapshot_ref",
        "lineage_edges",
        "pipeline_runs",
    ):
        identifier = (namespace, table_name)
        if catalog.table_exists(identifier):
            catalog.purge_table(identifier)
    if (namespace,) in catalog.list_namespaces():
        catalog.drop_namespace((namespace,))
PY

printf '%s\n' 'DQ quarantine smoke: passed.'
