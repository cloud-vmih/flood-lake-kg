"""Deterministic static-data quality assurance artifacts."""

from flashflood_data.static.qa.handler import task17_qa_handler
from flashflood_data.static.qa.map import QA_MAP_BUNDLE_RELATIVE_PATHS, publish_qa_map
from flashflood_data.static.qa.models import CheckResult, QAReport, QualityGateFailure
from flashflood_data.static.qa.report import publish_report
from flashflood_data.static.qa.runner import run_quality_gates

__all__ = ["QA_MAP_BUNDLE_RELATIVE_PATHS", "CheckResult", "QAReport", "QualityGateFailure", "publish_qa_map", "publish_report", "run_quality_gates", "task17_qa_handler"]
