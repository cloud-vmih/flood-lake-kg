from __future__ import annotations

from datetime import UTC, datetime

from flashflood_data.shared.quality import QualityEvaluation, QualityFinding
from flashflood_data.shared.quality.audit import QualityAuditContext, quality_rows

NOW = datetime(2026, 10, 11, tzinfo=UTC)


def test_quality_rows_preserve_version_scope_and_snapshot() -> None:
    finding = QualityFinding(
        rule_id="geometry_valid",
        rule_version="v2",
        status="failed",
        severity="warning",
        scope_key="feature-7",
        observed_value={"repaired": True},
        expected_value={"valid": True},
        failed_row_count=1,
    )
    evaluation = QualityEvaluation.from_parts(
        accepted_rows=[{"id": "feature-7", "quality_status": "warning"}],
        rejected_rows=[],
        findings=[finding],
    )

    rows = quality_rows(
        evaluation,
        QualityAuditContext(
            pipeline_run_id="run-1",
            dataset_id="bronze.river_reach_raw",
            check_phase="post_commit",
            checked_at=NOW,
            snapshot_table="bronze.river_reach_raw",
            snapshot_id=17,
        ),
    )

    assert rows[0]["rule_version"] == "v2"
    assert rows[0]["scope_key"] == "feature-7"
    assert rows[0]["observed_value_json"] == '{"repaired": true}'
    assert rows[0]["snapshot_id"] == 17

