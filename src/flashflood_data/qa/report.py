"""Deterministic, credential-safe publication of QA reports."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from flashflood_data.io_atomic import atomic_target
from flashflood_data.qa.checks import QAReport

_BEARER = re.compile(r"(?i)\bbearer\s+[^\s<,;]+")
_USERINFO = re.compile(r"(?i)(https?://)[^/\s@]+@")
_QUERY_FIELD = re.compile(r"([?&])([^=&#\s]+)=([^&#\s]*)")
_SENSITIVE_PARTS = (
    "secret",
    "token",
    "password",
    "authorization",
    "apikey",
    "signature",
    "credential",
)


def redact(value: object) -> str:
    """Redact credentials and signed URL parameters before QA publication."""
    text = _BEARER.sub("Bearer [REDACTED]", str(value))
    text = _USERINFO.sub(r"\1[REDACTED]@", text)

    def replace_query(match: re.Match[str]) -> str:
        normalized = re.sub(r"[^a-z0-9]", "", match.group(2).lower())
        if any(part in normalized for part in _SENSITIVE_PARTS):
            return f"{match.group(1)}{match.group(2)}=[REDACTED]"
        return match.group(0)

    return _QUERY_FIELD.sub(replace_query, text)


def _rows(report: QAReport) -> list[dict[str, object]]:
    return [
        {
            "check_id": check.check_id,
            "passed": check.passed,
            "severity": check.severity,
            "expected": redact(check.expected),
            "actual": redact(check.actual),
            "message": redact(check.message),
            "asset_ids": [redact(asset_id) for asset_id in check.asset_ids],
        }
        for check in sorted(report.checks, key=lambda check: check.check_id)
    ]


def _template(name: str):
    return Environment(
        loader=FileSystemLoader(Path(__file__).with_name("templates")),
        autoescape=select_autoescape(("html", "xml")),
    ).get_template(name)


def publish_report(report: QAReport, qa_dir: Path) -> list[Path]:
    """Write JSON, flat Parquet and linked HTML from a fully evaluated report."""
    qa_dir.mkdir(parents=True, exist_ok=True)
    rows = _rows(report)
    json_path = qa_dir / "report.json"
    parquet_path = qa_dir / "report.parquet"
    html_path = qa_dir / "report.html"
    payload = {
        "config_fingerprint": report.config_fingerprint,
        "run_id": report.run_id,
        "checks": rows,
        "artifacts": {"html": "report.html", "json": "report.json", "parquet": "report.parquet"},
    }
    with atomic_target(json_path) as partial:
        partial.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    with atomic_target(parquet_path) as partial:
        pd.DataFrame(rows).to_parquet(partial, index=False)
    grouped = {
        severity: [row for row in rows if row["severity"] == severity]
        for severity in ("fatal", "warning", "info")
    }
    html = _template("report.html.j2").render(report=payload, grouped=grouped)
    with atomic_target(html_path) as partial:
        partial.write_text(html, encoding="utf-8")
    return sorted([html_path, json_path, parquet_path], key=lambda path: path.name)
