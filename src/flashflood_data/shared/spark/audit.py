"""Repairable Meta audit lifecycle for Spark Silver builds."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from flashflood_data.shared.spark.models import PinnedSnapshot, PublishedSnapshot

_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10


class SparkMetaWriter(Protocol):
    def record_run(self, row: Mapping[str, Any]) -> int: ...

    def record_snapshot_refs(self, rows: Sequence[Mapping[str, Any]]) -> int: ...

    def record_qualities(self, rows: Sequence[Mapping[str, Any]]) -> int: ...

    def record_lineages(self, rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]: ...


@dataclass(frozen=True)
class SparkAuditRequest:
    pipeline_run_id: str
    orchestrator_run_id: str | None
    job_name: str
    build_signature: str
    config_hash: str
    policy_id: str
    algorithm_version: str
    input_snapshots: tuple[PinnedSnapshot, ...]
    required_outputs: tuple[str, ...]
    started_at: datetime
    input_object_ids: tuple[str, ...] = ()
    retry_count: int = 0
    code_git_sha: str | None = None
    image_digest: str | None = None

    def __post_init__(self) -> None:
        strings = (
            self.pipeline_run_id,
            self.job_name,
            self.build_signature,
            self.config_hash,
            self.policy_id,
            self.algorithm_version,
        )
        if any(not value for value in strings):
            raise ValueError("Spark audit identity fields are required")
        if not self.input_snapshots or not self.required_outputs:
            raise ValueError("Spark audit requires inputs and outputs")
        if len(set(self.required_outputs)) != len(self.required_outputs):
            raise ValueError("Spark audit required outputs must be unique")
        if (
            len(set(self.input_object_ids)) != len(self.input_object_ids)
            or any(not value for value in self.input_object_ids)
        ):
            raise ValueError("Spark audit input object IDs must be unique and non-empty")
        if self.started_at.tzinfo is None or self.started_at.utcoffset() != _UTC.utcoffset(
            self.started_at
        ):
            raise ValueError("Spark audit started_at must use timezone-aware UTC")


class SparkAudit:
    """Record a Spark build so success is always the final durable Meta event."""

    def __init__(
        self,
        meta: SparkMetaWriter,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.meta = meta
        self.now = now or (lambda: datetime.now(_UTC))

    @staticmethod
    def _metrics(
        request: SparkAuditRequest,
        outputs: Sequence[PublishedSnapshot] = (),
    ) -> str:
        return json.dumps(
            {
                "algorithm_version": request.algorithm_version,
                "build_signature": request.build_signature,
                "output_snapshots": {
                    output.table: output.snapshot_id for output in outputs
                },
                "policy_id": request.policy_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def _run_row(
        self,
        request: SparkAuditRequest,
        *,
        status: str,
        finished_at: datetime | None,
        outputs: Sequence[PublishedSnapshot] = (),
        error_code: str | None = None,
    ) -> dict[str, object]:
        succeeded = status in {"succeeded", "succeeded_with_warning"}
        return {
            "pipeline_run_id": request.pipeline_run_id,
            "orchestrator_run_id": request.orchestrator_run_id,
            "job_name": request.job_name,
            "code_git_sha": request.code_git_sha,
            "image_digest": request.image_digest,
            "config_hash": request.config_hash,
            "parameter_set_id": None,
            "started_at": request.started_at,
            "finished_at": finished_at,
            "published_at": finished_at if succeeded else None,
            "status": status,
            "retry_count": request.retry_count,
            "input_row_count": None,
            "output_row_count": sum(output.row_count for output in outputs) if succeeded else None,
            "quality_result_json": json.dumps(
                {"policy_id": request.policy_id}, separators=(",", ":")
            ),
            "metrics_json": self._metrics(request, outputs),
            "error_code": error_code,
        }

    def start(self, request: SparkAuditRequest) -> None:
        self.meta.record_run(
            self._run_row(request, status="running", finished_at=None)
        )

    def finalize_success(
        self,
        request: SparkAuditRequest,
        outputs: Sequence[PublishedSnapshot],
        *,
        auxiliary_outputs: Sequence[PublishedSnapshot] = (),
        quality_rows: Sequence[Mapping[str, Any]] = (),
    ) -> None:
        published = tuple(outputs)
        auxiliary = tuple(auxiliary_outputs)
        quality = tuple(quality_rows)
        by_table = {output.table: output for output in published}
        auxiliary_by_table = {output.table: output for output in auxiliary}
        missing = tuple(
            table for table in request.required_outputs if table not in by_table
        )
        unexpected = tuple(
            table for table in by_table if table not in request.required_outputs
        )
        if missing:
            raise ValueError(f"missing required outputs: {missing}")
        if unexpected or len(by_table) != len(published):
            raise ValueError("published output tables must be unique and declared")
        if (
            len(auxiliary_by_table) != len(auxiliary)
            or set(by_table) & set(auxiliary_by_table)
        ):
            raise ValueError("auxiliary output tables must be unique and separate")
        if not quality:
            raise ValueError("Spark audit success requires durable quality results")

        finished_at = self.now()
        has_warning = any(
            row.get("severity") == "warning" and row.get("status") == "failed"
            for row in quality
        )
        quality_status = "warning" if has_warning else "passed"
        snapshot_refs = tuple(
            {
                "pipeline_run_id": request.pipeline_run_id,
                "table_name": snapshot.table,
                "iceberg_snapshot_id": snapshot.snapshot_id,
                "role": "input",
                "quality_status": quality_status,
                "created_at": finished_at,
            }
            for snapshot in request.input_snapshots
        ) + tuple(
            {
                "pipeline_run_id": request.pipeline_run_id,
                "table_name": output.table,
                "iceberg_snapshot_id": output.snapshot_id,
                "role": "output",
                "quality_status": quality_status,
                "created_at": finished_at,
            }
            for output in (*published, *auxiliary)
        )
        self.meta.record_snapshot_refs(snapshot_refs)
        self.meta.record_qualities(quality)
        snapshot_lineages = tuple(
                {
                    "pipeline_run_id": request.pipeline_run_id,
                    "input_object_id": None,
                    "input_table": snapshot.table,
                    "input_snapshot_id": snapshot.snapshot_id,
                    "output_table": output.table,
                    "output_snapshot_id": output.snapshot_id,
                    "transform_role": request.job_name,
                    "mapping_version": request.algorithm_version,
                    "created_at": finished_at,
                }
                for snapshot in request.input_snapshots
                for output in (*published, *auxiliary)
            )
        object_lineages = tuple(
            {
                "pipeline_run_id": request.pipeline_run_id,
                "input_object_id": object_id,
                "input_table": None,
                "input_snapshot_id": None,
                "output_table": output.table,
                "output_snapshot_id": output.snapshot_id,
                "transform_role": request.job_name,
                "mapping_version": request.algorithm_version,
                "created_at": finished_at,
            }
            for object_id in request.input_object_ids
            for output in (*published, *auxiliary)
        )
        self.meta.record_lineages(snapshot_lineages + object_lineages)
        self.meta.record_run(
            self._run_row(
                request,
                status="succeeded_with_warning" if has_warning else "succeeded",
                finished_at=finished_at,
                outputs=published,
            )
        )

    def finalize_failure(self, request: SparkAuditRequest, error_code: str) -> None:
        if not error_code:
            raise ValueError("Spark audit failure requires error_code")
        self.meta.record_run(
            self._run_row(
                request,
                status="failed",
                finished_at=self.now(),
                error_code=error_code,
            )
        )
