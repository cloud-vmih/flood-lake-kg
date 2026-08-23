"""Deterministic static-data quality assurance artifacts."""

from flashflood_data.qa.checks import CheckResult, QAReport, run_quality_gates, task17_qa_handler
from flashflood_data.qa.map import publish_qa_map
from flashflood_data.qa.report import publish_report

__all__ = [
    "CheckResult",
    "QAReport",
    "publish_qa_map",
    "publish_report",
    "run_quality_gates",
    "task17_qa_handler",
]
