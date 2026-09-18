"""Airflow orchestration for immutable static source landing."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task, task_group

from flashflood_data.cli.app import build_static_landing_service
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.orchestration.landing.config import load_static_landing_config
from flashflood_data.orchestration.landing.models import (
    LandingRunSummary,
    LandingTaskEnvelope,
    PublishedBatch,
    RegisteredBatch,
)
from flashflood_data.orchestration.landing.service import (
    StaticSourceLandingService,
    cleanup_committed_staging,
    cleanup_failed_staging,
)

WRITER_POOL = "source_landing_writer"
EXPECTED_SOURCE_IDS = (
    "hydrobasins_v1c",
    "basinatlas_v10",
    "hydrorivers_v10",
    "cop_dem_glo30_2024_1",
    "soilgrids_2_0",
)
CONFIG_PATH = ProjectPaths.discover().root / "config" / "landing" / "static.yaml"


def _staging_root() -> Path:
    configured = os.environ.get("FLASHFLOOD_STAGING_ROOT")
    if configured:
        return Path(configured).resolve()
    return ProjectPaths.discover().dataset / "lakehouse" / "staging"


def _configured_source_ids() -> tuple[str, ...]:
    config = load_static_landing_config(CONFIG_PATH)
    source_ids = tuple(source.source_id for source in config.sources)
    if source_ids != EXPECTED_SOURCE_IDS:
        raise RuntimeError("static landing DAG source groups do not match the approved config")
    return source_ids


@task(retries=2)
def publish_source(source_id: str, landing_run_id: str) -> dict[str, object]:
    """Publish one source or return a sanitized failure envelope."""
    try:
        service = build_static_landing_service()
        batch = service.publish_source(source_id, landing_run_id)
        envelope = LandingTaskEnvelope.succeeded("published", batch)
    except Exception as error:  # noqa: BLE001 - preserve independent source groups
        envelope = LandingTaskEnvelope.failed(
            source_id, StaticSourceLandingService._error_code(error)
        )
    return envelope.model_dump(mode="json")


@task(retries=2)
def register_batch(envelope_json: dict[str, object]) -> dict[str, object]:
    """Register a published source batch or pass its failure through."""
    source_id = str(envelope_json.get("source_id", "unknown_source"))
    try:
        envelope = LandingTaskEnvelope.model_validate(envelope_json)
        source_id = envelope.source_id
        if envelope.status == "failure":
            return envelope.model_dump(mode="json")
        if not isinstance(envelope.batch, PublishedBatch):
            return LandingTaskEnvelope.failed(
                envelope.source_id, "invalid_published_envelope"
            ).model_dump(mode="json")
        service = build_static_landing_service()
        registered = service.register_batch(envelope.batch)
        result = LandingTaskEnvelope.succeeded("registered", registered)
    except Exception as error:  # noqa: BLE001 - preserve independent source groups
        result = LandingTaskEnvelope.failed(
            source_id, StaticSourceLandingService._error_code(error)
        )
    return result.model_dump(mode="json")


@task(retries=2)
def cleanup_batch(
    envelope_json: dict[str, object], landing_run_id: str
) -> dict[str, object]:
    """Remove committed run staging or pass a prior failure through."""
    source_id = str(envelope_json.get("source_id", "unknown_source"))
    try:
        envelope = LandingTaskEnvelope.model_validate(envelope_json)
        source_id = envelope.source_id
        config = load_static_landing_config(CONFIG_PATH)
        staging_root = _staging_root()
        if envelope.status == "failure":
            cleanup_failed_staging(
                config, staging_root, envelope.source_id, landing_run_id
            )
            return envelope.model_dump(mode="json")
        if not isinstance(envelope.batch, RegisteredBatch):
            cleanup_failed_staging(
                config, staging_root, envelope.source_id, landing_run_id
            )
            return LandingTaskEnvelope.failed(
                envelope.source_id, "invalid_registered_envelope"
            ).model_dump(mode="json")
        cleanup_committed_staging(config, staging_root, envelope.batch)
        result = LandingTaskEnvelope.succeeded("cleaned", envelope.batch)
    except Exception as error:  # noqa: BLE001 - preserve independent source groups
        document = {
            "source_id": source_id,
            "error_code": StaticSourceLandingService._error_code(error),
        }
        raise AirflowException(json.dumps(document, sort_keys=True)) from None
    return result.model_dump(mode="json")


@task_group
def source_landing_group(source_id: str, landing_run_id: str):
    """Create the three metadata-only boundaries for one source."""
    published = publish_source.override(pool="source_landing_writer")(
        source_id, landing_run_id
    )
    registered = register_batch.override(pool="source_landing_writer")(published)
    cleaned = cleanup_batch.override(pool="source_landing_writer")(
        registered, landing_run_id
    )
    return published, cleaned


@task(trigger_rule="all_done", retries=0)
def publish_run_summary(
    landing_run_id: str,
    source_ids: tuple[str, ...],
    envelope_documents: list[dict[str, object] | None],
) -> dict[str, object]:
    """Emit a combined result and make an incomplete landing run visible."""
    envelopes = []
    for source_id, document in zip(source_ids, envelope_documents, strict=True):
        try:
            envelopes.append(LandingTaskEnvelope.model_validate(document))
        except (TypeError, ValueError):
            envelopes.append(
                LandingTaskEnvelope.failed(source_id, "upstream_task_failed")
            )
    completed = sorted(
        envelope.source_id for envelope in envelopes if envelope.status == "success"
    )
    failed = sorted(envelope.source_id for envelope in envelopes if envelope.status == "failure")
    errors = {
        envelope.source_id: str(envelope.error_code)
        for envelope in envelopes
        if envelope.status == "failure"
    }
    snapshots = {
        envelope.source_id: envelope.batch.snapshot_id
        for envelope in envelopes
        if isinstance(envelope.batch, RegisteredBatch) and envelope.batch.snapshot_id is not None
    }
    status = "completed" if not failed else "partial_failure" if completed else "failed"
    summary = LandingRunSummary(
        run_id=landing_run_id,
        status=status,
        completed_sources=tuple(completed),
        failed_sources=tuple(failed),
        errors=errors,
        snapshots=snapshots,
    )
    document = summary.model_dump(mode="json")
    print(json.dumps(document, sort_keys=True))
    if status != "completed":
        raise AirflowException(json.dumps(document, sort_keys=True))
    return document


@dag(
    dag_id="static_source_landing",
    description="Land immutable static source objects in MinIO and inventory them in Iceberg",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2},
    tags=["source", "static", "minio", "iceberg"],
)
def static_source_landing_dag():
    """Build fixed, independent task groups from the approved landing policy."""
    landing_run_id = "{{ run_id }}"
    source_ids = _configured_source_ids()
    results = []
    previous_cleanup = None
    for source_id in source_ids:
        published, cleaned = source_landing_group.override(group_id=source_id)(
            source_id, landing_run_id
        )
        if previous_cleanup is not None:
            previous_cleanup >> published
        results.append(cleaned)
        previous_cleanup = cleaned
    publish_run_summary(landing_run_id, source_ids, results)


static_source_landing = static_source_landing_dag()
