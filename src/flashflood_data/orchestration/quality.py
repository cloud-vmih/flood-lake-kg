"""Shared quality-result contract; each pipeline owns its actual rules."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class QualityResult:
    rule_id: str
    status: Literal["passed", "failed", "error", "skipped"]
    severity: Literal["fatal", "warning", "info"]
    observed_value: object
    failed_row_count: int | None = None


def fatal_failures(results: list[QualityResult]) -> list[QualityResult]:
    """Return blocking failures while retaining warnings for audit only."""
    return [
        result for result in results
        if result.severity == "fatal" and result.status in {"failed", "error"}
    ]
