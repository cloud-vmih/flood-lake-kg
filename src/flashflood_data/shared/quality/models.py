"""Immutable data-quality decision models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, TypeAlias

FindingStatus: TypeAlias = Literal["passed", "failed", "error", "skipped"]
Severity: TypeAlias = Literal["fatal", "reject", "warning", "info"]
Disposition: TypeAlias = Literal[
    "pass", "accept_with_warning", "reject_rows", "block_object"
]


@dataclass(frozen=True)
class QualityFinding:
    """Result of one versioned rule at a stable object or row scope.

    The first five fields preserve the positional constructor used by the
    existing Bronze pipeline. New code should pass every field by name.
    """

    rule_id: str
    status: FindingStatus
    severity: Severity
    observed_value: object
    failed_row_count: int | None = None
    rule_version: str = "v1"
    scope_key: str = ""
    expected_value: object | None = None


# Compatibility name used by the existing static Bronze implementation.
QualityResult = QualityFinding


@dataclass(frozen=True)
class RejectedRecord:
    """One source row excluded from a target business table."""

    source_record_key: str
    record: Mapping[str, object] | None
    record_uri: str | None
    findings: tuple[QualityFinding, ...]

    def __post_init__(self) -> None:
        if not self.source_record_key:
            raise ValueError("source_record_key is required")
        if self.record is None and self.record_uri is None:
            raise ValueError("a rejected record requires record or record_uri")


def _record_key(row: Mapping[str, object]) -> str | None:
    for field in ("source_record_key", "id", "object_id"):
        value = row.get(field)
        if value is not None:
            return str(value)
    return None


@dataclass(frozen=True)
class QualityEvaluation:
    """Deterministic partition of accepted rows, rejected rows, and findings."""

    accepted_rows: tuple[Mapping[str, object], ...]
    rejected_rows: tuple[RejectedRecord, ...]
    findings: tuple[QualityFinding, ...]
    disposition: Disposition

    @classmethod
    def from_parts(
        cls,
        *,
        accepted_rows: Sequence[Mapping[str, object]],
        rejected_rows: Sequence[RejectedRecord],
        findings: Sequence[QualityFinding],
        input_row_count: int | None = None,
    ) -> QualityEvaluation:
        accepted = tuple(dict(row) for row in accepted_rows)
        rejected = tuple(rejected_rows)
        collected = list(findings)

        accepted_keys = {key for row in accepted if (key := _record_key(row)) is not None}
        rejected_keys = {item.source_record_key for item in rejected}
        overlap = accepted_keys & rejected_keys
        if overlap:
            raise ValueError(
                "source records cannot be both accepted and rejected: "
                + ", ".join(sorted(overlap))
            )

        has_fatal = bool(fatal_failures(collected))
        total_rows = input_row_count
        if total_rows is None:
            total_rows = len(accepted) + len(rejected)
        if total_rows > 0 and not accepted and rejected and not has_fatal:
            collected.append(
                QualityFinding(
                    rule_id="no_accepted_rows",
                    rule_version="v1",
                    status="failed",
                    severity="fatal",
                    scope_key="object",
                    observed_value={"accepted_count": 0, "rejected_count": len(rejected)},
                    expected_value={"minimum_accepted_count": 1},
                    failed_row_count=len(rejected),
                )
            )
            has_fatal = True

        if has_fatal:
            disposition: Disposition = "block_object"
            accepted = ()
        elif rejected:
            disposition = "reject_rows"
        elif any(
            item.severity == "warning" and item.status in {"failed", "error"}
            for item in collected
        ):
            disposition = "accept_with_warning"
        else:
            disposition = "pass"

        return cls(
            accepted_rows=accepted,
            rejected_rows=rejected,
            findings=tuple(collected),
            disposition=disposition,
        )


def fatal_failures(results: Sequence[QualityFinding]) -> list[QualityFinding]:
    """Return blocking findings while retaining warnings for audit only."""
    return [
        result
        for result in results
        if result.severity == "fatal" and result.status in {"failed", "error"}
    ]
