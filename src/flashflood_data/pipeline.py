"""Compatibility exports for the static workflow."""
from flashflood_data.static.workflow import (
    STATIC_ORDER,
    RunSummary,
    Stage,
    StaticPipeline,
    dependency_fingerprint,
)

__all__ = ["STATIC_ORDER", "RunSummary", "Stage", "StaticPipeline", "dependency_fingerprint"]
