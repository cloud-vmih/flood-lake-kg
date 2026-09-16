"""Value objects for static quality assurance."""

from dataclasses import dataclass
from typing import Literal

Severity = Literal["info", "warning", "fatal"]


@dataclass(frozen=True)
class CheckResult:
    """One independently-readable gate result."""
    check_id: str
    passed: bool
    severity: Severity
    expected: str
    actual: str
    message: str
    asset_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class QAReport:
    """Stable QA report, including failures instead of throwing them away."""
    run_id: str
    config_fingerprint: str
    checks: tuple[CheckResult, ...]

    def by_id(self, check_id: str) -> CheckResult:
        return next(check for check in self.checks if check.check_id == check_id)

    @property
    def fatal_failures(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed and check.severity == "fatal")


class QualityGateFailure(RuntimeError):
    """Raised only after fatal QA reports and map artifacts were published."""
