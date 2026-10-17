"""Repairable publication coordinator for DQ-gated object slices."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol

from flashflood_data.shared.quality.audit import QualityAuditContext, quality_rows
from flashflood_data.shared.quality.models import QualityEvaluation
from flashflood_data.shared.quality.quarantine import (
    ObjectRejection,
    QuarantineRepository,
    RowRejection,
)

LOGGER = logging.getLogger(__name__)
_UTC = timezone.utc  # noqa: UP017 - imported by the Python 3.10 Spark driver


class TargetWriter(Protocol):
    def replace_object_rows(
        self,
        identifier: tuple[str, str],
        object_id: str,
        rows: tuple[Mapping[str, object], ...],
    ) -> int: ...


class MetaWriter(Protocol):
    def record_qualities(self, rows): ...

    def record_snapshot_ref(self, **row): ...

    def record_lineages(self, rows): ...

    def record_run(self, row): ...


@dataclass(frozen=True)
class DqPublishRequest:
    pipeline_run_id: str
    layer: Literal["raw", "bronze", "silver", "gold"]
    target_dataset_id: str
    object_id: str
    policy_id: str
    parser_version: str
    lineage_inputs: tuple[Mapping[str, object], ...] = ()
    source_id: str | None = None
    orchestrator_run_id: str | None = None
    job_name: str = "dq_publish"
    config_hash: str | None = None
    started_at: datetime | None = None
    retry_count: int = 0


@dataclass(frozen=True)
class PublishDecision:
    status: Literal["succeeded", "succeeded_with_warning", "failed_data_quality"]
    accepted_count: int
    rejected_count: int
    warning_count: int
    output_snapshot_id: int | None
    quarantine_snapshot_ids: tuple[int, ...]


class DqPublishCoordinator:
    """Publish quarantine, target rows, and Meta evidence in repairable order."""

    def __init__(
        self,
        *,
        target_writer: TargetWriter,
        quarantine: QuarantineRepository,
        meta: MetaWriter,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.target_writer = target_writer
        self.quarantine = quarantine
        self.meta = meta
        self.now = now or (lambda: datetime.now(_UTC))

    def _run_row(
        self,
        request: DqPublishRequest,
        *,
        status: str,
        evaluation: QualityEvaluation,
        finished_at: datetime,
        published: bool,
        error_code: str | None = None,
    ) -> dict[str, object]:
        warning_count = sum(
            row.get("quality_status") == "warning" for row in evaluation.accepted_rows
        )
        fatal_count = sum(
            finding.severity == "fatal" and finding.status in {"failed", "error"}
            for finding in evaluation.findings
        )
        return {
            "pipeline_run_id": request.pipeline_run_id,
            "orchestrator_run_id": request.orchestrator_run_id,
            "job_name": request.job_name,
            "code_git_sha": None,
            "image_digest": None,
            "config_hash": request.config_hash or request.policy_id,
            "parameter_set_id": None,
            "started_at": request.started_at or finished_at,
            "finished_at": finished_at,
            "published_at": finished_at if published else None,
            "status": status,
            "retry_count": request.retry_count,
            "input_row_count": len(evaluation.accepted_rows) + len(evaluation.rejected_rows),
            "output_row_count": len(evaluation.accepted_rows) if published else None,
            "quality_result_json": json.dumps(
                {"disposition": evaluation.disposition}, separators=(",", ":")
            ),
            "metrics_json": json.dumps(
                {
                    "accepted_count": len(evaluation.accepted_rows),
                    "rejected_count": len(evaluation.rejected_rows),
                    "warning_count": warning_count,
                    "fatal_count": fatal_count,
                    "quality_policy_id": request.policy_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            "error_code": error_code,
        }

    def publish_object(
        self, request: DqPublishRequest, evaluation: QualityEvaluation
    ) -> PublishDecision:
        now = self.now()
        quarantine_snapshots: list[int] = []
        warning_count = sum(
            row.get("quality_status") == "warning" for row in evaluation.accepted_rows
        )
        try:
            if evaluation.disposition == "block_object":
                audit_rows = quality_rows(
                    evaluation,
                    QualityAuditContext(
                        pipeline_run_id=request.pipeline_run_id,
                        dataset_id=request.target_dataset_id,
                        check_phase="pre_commit",
                        checked_at=now,
                    ),
                )
                if audit_rows:
                    self.meta.record_qualities(audit_rows)
                for finding in evaluation.findings:
                    if finding.severity != "fatal" or finding.status not in {"failed", "error"}:
                        continue
                    _, snapshot_id = self.quarantine.reject_object(
                        ObjectRejection(
                            pipeline_run_id=request.pipeline_run_id,
                            layer=request.layer,
                            target_dataset_id=request.target_dataset_id,
                            source_id=request.source_id,
                            object_id=request.object_id,
                            scope_key=finding.scope_key or request.object_id,
                            quality_policy_id=request.policy_id,
                            rule_id=finding.rule_id,
                            rule_version=finding.rule_version,
                            reason_code=finding.rule_id,
                            detail={
                                "observed_value": finding.observed_value,
                                "expected_value": finding.expected_value,
                            },
                            rejected_at=now,
                        )
                    )
                    quarantine_snapshots.append(snapshot_id)
                self.meta.record_run(
                    self._run_row(
                        request,
                        status="failed_data_quality",
                        evaluation=evaluation,
                        finished_at=now,
                        published=False,
                        error_code="data_quality_block",
                    )
                )
                return PublishDecision(
                    status="failed_data_quality",
                    accepted_count=0,
                    rejected_count=len(evaluation.rejected_rows),
                    warning_count=0,
                    output_snapshot_id=None,
                    quarantine_snapshot_ids=tuple(quarantine_snapshots),
                )

            if evaluation.rejected_rows:
                row_records = [
                    RowRejection(
                        pipeline_run_id=request.pipeline_run_id,
                        layer=request.layer,  # type: ignore[arg-type]
                        target_dataset_id=request.target_dataset_id,
                        object_id=request.object_id,
                        source_record_key=rejected.source_record_key,
                        quality_policy_id=request.policy_id,
                        findings=rejected.findings,
                        reason_codes=tuple(item.rule_id for item in rejected.findings),
                        record=rejected.record,
                        record_uri=rejected.record_uri,
                        parser_version=request.parser_version,
                        rejected_at=now,
                    )
                    for rejected in evaluation.rejected_rows
                ]
                _, quarantine_snapshot = self.quarantine.reject_rows(row_records)
                quarantine_snapshots.append(quarantine_snapshot)

            parts = request.target_dataset_id.split(".")
            if len(parts) == 2:
                namespace, table_name = parts
            elif len(parts) == 3:
                _catalog, namespace, table_name = parts
            else:
                raise ValueError(
                    "target_dataset_id must use namespace.table or catalog.namespace.table form"
                )
            if not namespace or not table_name:
                raise ValueError("target_dataset_id contains an empty namespace or table")
            output_snapshot_id = self.target_writer.replace_object_rows(
                (namespace, table_name), request.object_id, evaluation.accepted_rows
            )
            audit_rows = quality_rows(
                evaluation,
                QualityAuditContext(
                    pipeline_run_id=request.pipeline_run_id,
                    dataset_id=request.target_dataset_id,
                    check_phase="post_commit",
                    checked_at=now,
                    snapshot_table=request.target_dataset_id,
                    snapshot_id=output_snapshot_id,
                ),
            )
            if audit_rows:
                self.meta.record_qualities(audit_rows)
            status = (
                "succeeded_with_warning"
                if evaluation.rejected_rows or warning_count
                else "succeeded"
            )
            self.meta.record_snapshot_ref(
                pipeline_run_id=request.pipeline_run_id,
                table_name=request.target_dataset_id,
                iceberg_snapshot_id=output_snapshot_id,
                role="output",
                quality_status="warning" if status == "succeeded_with_warning" else "passed",
                created_at=now,
            )
            if request.lineage_inputs:
                self.meta.record_lineages(
                    [
                        {
                            **dict(lineage),
                            "pipeline_run_id": request.pipeline_run_id,
                            "output_table": request.target_dataset_id,
                            "output_snapshot_id": output_snapshot_id,
                            "mapping_version": lineage.get(
                                "mapping_version", request.parser_version
                            ),
                            "created_at": now,
                        }
                        for lineage in request.lineage_inputs
                    ]
                )
            self.meta.record_run(
                self._run_row(
                    request,
                    status=status,
                    evaluation=evaluation,
                    finished_at=now,
                    published=True,
                )
            )
            return PublishDecision(
                status=status,
                accepted_count=len(evaluation.accepted_rows),
                rejected_count=len(evaluation.rejected_rows),
                warning_count=warning_count,
                output_snapshot_id=output_snapshot_id,
                quarantine_snapshot_ids=tuple(quarantine_snapshots),
            )
        except Exception as error:
            try:
                self.meta.record_run(
                    self._run_row(
                        request,
                        status="failed",
                        evaluation=evaluation,
                        finished_at=self.now(),
                        published=False,
                        error_code=type(error).__name__,
                    )
                )
            except Exception:
                LOGGER.exception("could not record operational DQ publication failure")
            raise
