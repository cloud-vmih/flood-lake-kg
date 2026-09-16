"""Compatibility exports for static quality checks."""
from flashflood_data.static.qa.handler import task17_qa_handler
from flashflood_data.static.qa.models import CheckResult, QAReport, QualityGateFailure
from flashflood_data.static.qa.runner import run_quality_gates

__all__ = ["CheckResult", "QAReport", "QualityGateFailure", "run_quality_gates", "task17_qa_handler"]
