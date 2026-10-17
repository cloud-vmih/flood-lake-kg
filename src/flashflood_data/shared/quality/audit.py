"""Conversion from shared DQ findings to the Meta physical contract."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from flashflood_data.shared.quality.models import QualityEvaluation


@dataclass(frozen=True)
class QualityAuditContext:
    pipeline_run_id: str
    dataset_id: str
    check_phase: Literal["pre_commit", "post_commit", "pre_publish"]
    checked_at: datetime
    snapshot_table: str | None = None
    snapshot_id: int | None = None

    def __post_init__(self) -> None:
        if (self.snapshot_table is None) != (self.snapshot_id is None):
            raise ValueError("snapshot_table and snapshot_id must be supplied together")


def _json(value: object | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, default=str)


def quality_rows(
    evaluation: QualityEvaluation, context: QualityAuditContext
) -> list[dict[str, object]]:
    """Return rows accepted by ``meta.quality_results``."""
    return [
        {
            "pipeline_run_id": context.pipeline_run_id,
            "check_phase": context.check_phase,
            "dataset_id": context.dataset_id,
            "rule_id": finding.rule_id,
            "rule_version": finding.rule_version,
            "scope_key": finding.scope_key or "all",
            "severity": finding.severity,
            "status": finding.status,
            "observed_value_json": _json(finding.observed_value),
            "expected_value_json": _json(finding.expected_value),
            "failed_row_count": finding.failed_row_count,
            "sample_uri": None,
            "snapshot_table": context.snapshot_table,
            "snapshot_id": context.snapshot_id,
            "checked_at": context.checked_at,
        }
        for finding in evaluation.findings
    ]
