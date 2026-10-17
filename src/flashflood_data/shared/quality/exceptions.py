"""Exceptions that distinguish deterministic data failures from infrastructure failures."""

from __future__ import annotations

from flashflood_data.shared.quality.models import QualityEvaluation


class DataQualityBlock(ValueError):
    """A deterministic input defect blocked publication and must not be retried as I/O."""

    def __init__(
        self,
        message: str,
        evaluation: QualityEvaluation,
        *,
        evidence_uri: str | None = None,
    ) -> None:
        super().__init__(message)
        self.evaluation = evaluation
        self.evidence_uri = evidence_uri
