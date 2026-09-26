"""Audit successful raw registration in the existing static landing pipeline."""

import json
import logging
from collections.abc import Sequence
from datetime import datetime
from hashlib import sha256
from time import monotonic

from flashflood_data.orchestration.landing.models import RegisteredBatch, SourceObjectRow
from flashflood_data.orchestration.meta.service import MetaRecorder

LOGGER = logging.getLogger(__name__)


def audit_registered_batch(
    batch: RegisteredBatch,
    rows: Sequence[SourceObjectRow],
    meta: MetaRecorder,
    *,
    catalog_name: str,
    checked_at: datetime,
    attempt_no: int = 1,
) -> None:
    """Record asset-registration attempts and the source-object output snapshot."""
    if batch.snapshot_id is None:
        raise ValueError("cannot audit raw registration without an Iceberg snapshot")
    selected = [row for row in rows if row.object_id in batch.object_ids]
    if {row.object_id for row in selected} != set(batch.object_ids):
        raise ValueError("registered batch has missing available source objects")
    if any(row.source_id != batch.source_id for row in selected):
        raise ValueError("registered batch contains another source")
    dataset_id = f"{catalog_name}.meta.source_objects"
    pipeline_run_id = sha256(
        f"raw_landing:{batch.run_id}:{batch.source_id}".encode()
    ).hexdigest()
    attempts = []
    quality_results = []
    for row in selected:
        request = json.dumps(
            {"source_uri": row.source_uri, "selection_json": row.selection_json},
            sort_keys=True,
        )
        attempts.append(
            {
                "ingest_run_id": batch.run_id,
                "source_id": batch.source_id,
                "asset_id": row.asset_id,
                "attempt_no": attempt_no,
                "request_fingerprint": sha256(request.encode("utf-8")).hexdigest(),
                "http_status": None,
                "error_code": None,
                "started_at": checked_at,
                "ended_at": checked_at,
                "status": "succeeded",
            }
        )
        quality_results.append(
            {
                "pipeline_run_id": pipeline_run_id,
                "check_phase": "post_commit",
                "dataset_id": dataset_id,
                "rule_id": "verified_object_registration",
                "rule_version": "v1",
                "scope_key": row.object_id,
                "severity": "fatal",
                "status": "passed",
                "observed_value_json": json.dumps(
                    {"size_bytes": row.size_bytes, "checksum": row.checksum}
                ),
                "expected_value_json": None,
                "failed_row_count": None,
                "sample_uri": None,
                "snapshot_table": dataset_id,
                "snapshot_id": batch.snapshot_id,
                "checked_at": checked_at,
            }
        )
    started = monotonic()
    meta.record_attempts(attempts)
    LOGGER.info(
        "raw Meta audit phase complete: source_id=%s phase=attempts rows=%d duration_s=%.3f",
        batch.source_id,
        len(attempts),
        monotonic() - started,
    )
    started = monotonic()
    meta.record_qualities(quality_results)
    LOGGER.info(
        "raw Meta audit phase complete: source_id=%s phase=quality rows=%d duration_s=%.3f",
        batch.source_id,
        len(quality_results),
        monotonic() - started,
    )
    started = monotonic()
    meta.record_snapshot_ref(
        pipeline_run_id=pipeline_run_id,
        table_name=dataset_id,
        iceberg_snapshot_id=batch.snapshot_id,
        role="output",
        quality_status="passed",
        created_at=checked_at,
    )
    LOGGER.info(
        "raw Meta audit phase complete: source_id=%s phase=snapshot_ref duration_s=%.3f",
        batch.source_id,
        monotonic() - started,
    )
    started = monotonic()
    meta.record_run({
        "pipeline_run_id": pipeline_run_id,
        "orchestrator_run_id": batch.run_id,
        "job_name": f"raw_landing:{batch.source_id}",
        "code_git_sha": None,
        "image_digest": None,
        "config_hash": sha256(batch.source_id.encode("utf-8")).hexdigest(),
        "parameter_set_id": None,
        "started_at": checked_at,
        "finished_at": checked_at,
        "published_at": checked_at,
        "status": "succeeded",
        "retry_count": attempt_no - 1,
        "input_row_count": len(selected),
        "output_row_count": len(selected),
        "quality_result_json": '{"status":"passed"}',
        "metrics_json": None,
        "error_code": None,
    })
    LOGGER.info(
        "raw Meta audit phase complete: source_id=%s phase=pipeline_run duration_s=%.3f",
        batch.source_id,
        monotonic() - started,
    )
