from __future__ import annotations

from flashflood_data.shared.quality import (
    QualityEvaluation,
    QualityFinding,
    evaluate_rows,
)


def finding(
    rule_id: str,
    severity: str,
    status: str,
    *,
    scope_key: str = "object-1",
) -> QualityFinding:
    return QualityFinding(
        rule_id=rule_id,
        status=status,
        severity=severity,
        observed_value={"fixture": True},
        rule_version="v1",
        scope_key=scope_key,
    )


def test_fatal_finding_blocks_object() -> None:
    result = QualityEvaluation.from_parts(
        accepted_rows=[{"id": "1"}],
        rejected_rows=[],
        findings=[finding("crs_usable", "fatal", "failed")],
    )

    assert result.disposition == "block_object"
    assert result.accepted_rows == ()


def test_reject_keeps_valid_rows_and_warning_marks_accepted_row() -> None:
    def fixture_validator(row: dict[str, object]):
        if row["id"] == "bad":
            return [finding("valid_geometry", "reject", "failed", scope_key="bad")]
        if row["id"] == "warn":
            return [finding("geometry_repaired", "warning", "failed", scope_key="warn")]
        return [finding("valid_geometry", "reject", "passed", scope_key=str(row["id"]))]

    result = evaluate_rows(
        [{"id": "ok"}, {"id": "bad"}, {"id": "warn"}],
        validators=(fixture_validator,),
    )

    assert [row["id"] for row in result.accepted_rows] == ["ok", "warn"]
    assert result.accepted_rows[0]["quality_status"] == "passed"
    assert result.accepted_rows[1]["quality_status"] == "warning"
    assert [item.source_record_key for item in result.rejected_rows] == ["bad"]
    assert result.disposition == "reject_rows"


def test_zero_accepted_rows_becomes_fatal() -> None:
    def reject_all(row: dict[str, object]):
        return [
            finding(
                "required_business_key",
                "reject",
                "failed",
                scope_key=str(row["id"]),
            )
        ]

    result = evaluate_rows([{"id": "bad"}], validators=(reject_all,))

    assert result.disposition == "block_object"
    assert result.accepted_rows == ()
    assert any(item.rule_id == "no_accepted_rows" for item in result.findings)


def test_fatal_row_finding_discards_all_rows() -> None:
    def fatal_on_second(row: dict[str, object]):
        if row["id"] == "second":
            return [finding("schema_usable", "fatal", "error", scope_key="second")]
        return []

    result = evaluate_rows(
        [{"id": "first"}, {"id": "second"}],
        validators=(fatal_on_second,),
    )

    assert result.disposition == "block_object"
    assert result.accepted_rows == ()

