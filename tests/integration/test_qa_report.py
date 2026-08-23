"""Publication contracts for deterministic QA reports."""

import json
from pathlib import Path

import pandas as pd

from flashflood_data.qa.checks import CheckResult, QAReport
from flashflood_data.qa.report import publish_report


def test_report_publishes_sorted_redacted_json_parquet_and_html(tmp_path: Path) -> None:
    report = QAReport(
        run_id="run-qa",
        config_fingerprint="config-fingerprint",
        checks=(
            CheckResult("z.warning", False, "warning", "x", "x", "bearer secret-token"),
            CheckResult("a.fatal", False, "fatal", "yes", "no", "https://host/x?token=abc"),
        ),
    )

    outputs = publish_report(report, tmp_path / "dataset" / "qa")

    assert [path.name for path in outputs] == ["report.html", "report.json", "report.parquet"]
    payload = json.loads((tmp_path / "dataset" / "qa" / "report.json").read_text())
    assert [check["check_id"] for check in payload["checks"]] == ["a.fatal", "z.warning"]
    assert "secret-token" not in json.dumps(payload)
    assert "token=abc" not in json.dumps(payload)
    assert set(pd.read_parquet(tmp_path / "dataset" / "qa" / "report.parquet").columns) >= {
        "check_id",
        "severity",
        "passed",
    }
    assert "report.parquet" in (tmp_path / "dataset" / "qa" / "report.html").read_text()
