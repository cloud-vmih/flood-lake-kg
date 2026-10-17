"""Helpers for applying row validators and selecting a DQ disposition."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TypeAlias

from flashflood_data.shared.quality.models import (
    Disposition,
    QualityEvaluation,
    QualityFinding,
    RejectedRecord,
)

RowValidator: TypeAlias = Callable[
    [Mapping[str, object]], QualityFinding | Iterable[QualityFinding] | None
]


def finding_disposition(finding: QualityFinding) -> Disposition:
    """Map one finding to the strongest action it requires."""
    if finding.status not in {"failed", "error"}:
        return "pass"
    return {
        "fatal": "block_object",
        "reject": "reject_rows",
        "warning": "accept_with_warning",
        "info": "pass",
    }[finding.severity]


def _record_key(row: Mapping[str, object]) -> str:
    for field in ("source_record_key", "id", "object_id"):
        value = row.get(field)
        if value is not None:
            return str(value)
    canonical = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _findings(value: QualityFinding | Iterable[QualityFinding] | None) -> tuple[QualityFinding, ...]:
    if value is None:
        return ()
    if isinstance(value, QualityFinding):
        return (value,)
    return tuple(value)


def evaluate_rows(
    rows: Sequence[Mapping[str, object]],
    validators: Sequence[RowValidator],
) -> QualityEvaluation:
    """Run domain validators and partition rows without losing row evidence."""
    accepted: list[Mapping[str, object]] = []
    rejected: list[RejectedRecord] = []
    findings: list[QualityFinding] = []

    for source_row in rows:
        row = dict(source_row)
        row_findings: list[QualityFinding] = []
        for validator in validators:
            row_findings.extend(_findings(validator(row)))
        findings.extend(row_findings)

        failed = [
            item for item in row_findings if item.status in {"failed", "error"}
        ]
        if any(item.severity == "reject" for item in failed):
            rejected.append(
                RejectedRecord(
                    source_record_key=_record_key(row),
                    record=row,
                    record_uri=None,
                    findings=tuple(item for item in failed if item.severity == "reject"),
                )
            )
            continue

        row["quality_status"] = (
            "warning" if any(item.severity == "warning" for item in failed) else "passed"
        )
        accepted.append(row)

    return QualityEvaluation.from_parts(
        accepted_rows=accepted,
        rejected_rows=rejected,
        findings=findings,
        input_row_count=len(rows),
    )
