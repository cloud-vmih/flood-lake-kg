"""Versioned quality rules for Bronze weather raster slices."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from flashflood_data.shared.quality import (
    QualityEvaluation,
    QualityFinding,
    QualityPolicy,
    QualityRule,
    RejectedRecord,
)

_FLUX_VARIABLES = frozenset(
    {
        "precipitation",
        "total_precipitation",
        "runoff",
        "surface_runoff",
        "sub_surface_runoff",
    }
)


def default_weather_quality_policy() -> QualityPolicy:
    """Return the fallback policy used by library callers and isolated tests."""
    severities = {
        "object_checksum": "fatal",
        "object_readable": "fatal",
        "nonempty_weather_parse": "fatal",
        "parallel_array_lengths": "fatal",
        "source_grid_complete": "fatal",
        "unit_present": "fatal",
        "temporal_window_valid": "fatal",
        "nonnegative_flux": "reject",
        "negative_flux_clamped": "warning",
        "null_cell_ratio": "warning",
    }
    return QualityPolicy(
        policy_name="bronze_weather",
        policy_version="v1",
        layer="bronze",
        rules=tuple(
            QualityRule(rule_id=rule_id, rule_version="v1", severity=severity)  # type: ignore[arg-type]
            for rule_id, severity in severities.items()
        ),
        defaults={
            "published_quality_status": ["passed", "warning"],
            "negative_flux_tolerance": 1.0e-6,
            "maximum_null_cell_ratio": 0.05,
        },
        policy_id="bronze_weather:v1:built-in",
    )


def _rule(policy: QualityPolicy, rule_id: str, default: str) -> tuple[str, str]:
    configured = next(
        (item for item in policy.rules if item.rule_id == rule_id and item.enabled), None
    )
    if configured is None:
        return "v1", default
    return configured.rule_version, configured.severity


def _finding(
    policy: QualityPolicy,
    rule_id: str,
    *,
    passed: bool,
    scope_key: str,
    observed: object,
    expected: object | None,
    default_severity: str,
    failed_row_count: int = 1,
) -> QualityFinding:
    version, severity = _rule(policy, rule_id, default_severity)
    return QualityFinding(
        rule_id=rule_id,
        rule_version=version,
        status="passed" if passed else "failed",
        severity=severity,  # type: ignore[arg-type]
        scope_key=scope_key,
        observed_value=observed,
        expected_value=expected,
        failed_row_count=0 if passed else failed_row_count,
    )


def _source_record_key(row: Mapping[str, object]) -> str:
    return "|".join(
        str(row.get(name) or "")
        for name in (
            "object_id",
            "valid_time",
            "source_revision",
            "vertical_level",
            "variable",
        )
    )


def evaluate_weather_slices(
    rows: Sequence[Mapping[str, object]],
    allowed_indices: set[int],
    policy: QualityPolicy,
    *,
    object_context: Mapping[str, object] | None = None,
) -> QualityEvaluation:
    """Partition slices while preserving parallel grid/value array alignment."""
    findings: list[QualityFinding] = []
    accepted: list[Mapping[str, object]] = []
    rejected: list[RejectedRecord] = []
    context = object_context or {}
    object_id = str(context.get("object_id") or "object")
    for rule_id, context_key in (
        ("object_checksum", "checksum_valid"),
        ("object_readable", "readable"),
    ):
        valid = bool(context.get(context_key, True))
        observed: dict[str, object] = {context_key: valid}
        if not valid and context.get("error"):
            observed["error"] = str(context["error"])
        findings.append(
            _finding(
                policy,
                rule_id,
                passed=valid,
                scope_key=object_id,
                observed=observed,
                expected={context_key: True},
                default_severity="fatal",
            )
        )
    if not rows:
        findings.append(
            _finding(
                policy,
                "nonempty_weather_parse",
                passed=False,
                scope_key=object_id,
                observed={"slice_count": 0},
                expected={"minimum_slice_count": 1},
                default_severity="fatal",
            )
        )
        return QualityEvaluation.from_parts(
            accepted_rows=(), rejected_rows=(), findings=findings, input_row_count=0
        )

    tolerance = float(policy.defaults.get("negative_flux_tolerance", 1.0e-6))
    maximum_null_ratio = float(policy.defaults.get("maximum_null_cell_ratio", 0.05))
    for source_row in rows:
        row = dict(source_row)
        scope_key = _source_record_key(row)
        indices = list(map(int, row.get("cell_indices") or ()))
        values = list(row.get("values") or ())
        row_findings: list[QualityFinding] = []

        arrays_aligned = len(indices) == len(values)
        row_findings.append(
            _finding(
                policy,
                "parallel_array_lengths",
                passed=arrays_aligned,
                scope_key=scope_key,
                observed={"cell_indices": len(indices), "values": len(values)},
                expected={"equal_lengths": True},
                default_severity="fatal",
            )
        )
        grid_complete = (
            arrays_aligned
            and len(indices) == len(set(indices))
            and set(indices) == allowed_indices
        )
        row_findings.append(
            _finding(
                policy,
                "source_grid_complete",
                passed=grid_complete,
                scope_key=scope_key,
                observed={
                    "cell_count": len(indices),
                    "unique_cell_count": len(set(indices)),
                    "expected_cell_count": len(allowed_indices),
                },
                expected={"indices_match_registered_scope": True},
                default_severity="fatal",
            )
        )
        unit_present = bool(str(row.get("unit") or "").strip())
        row_findings.append(
            _finding(
                policy,
                "unit_present",
                passed=unit_present,
                scope_key=scope_key,
                observed={"unit": row.get("unit")},
                expected={"nonempty": True},
                default_severity="fatal",
            )
        )
        start = row.get("window_start")
        end = row.get("window_end")
        temporal_valid = start is not None and end is not None
        if temporal_valid:
            try:
                temporal_valid = bool(end >= start)  # type: ignore[operator]
            except TypeError:
                temporal_valid = False
        row_findings.append(
            _finding(
                policy,
                "temporal_window_valid",
                passed=temporal_valid,
                scope_key=scope_key,
                observed={"window_start": start, "window_end": end},
                expected={"window_end_gte_start": True},
                default_severity="fatal",
            )
        )

        warning = False
        variable = str(row.get("variable") or "")
        if variable in _FLUX_VARIABLES and arrays_aligned:
            negative = [float(value) for value in values if value is not None and float(value) < 0]
            severe_negative = [value for value in negative if value < -tolerance]
            if severe_negative:
                row_findings.append(
                    _finding(
                        policy,
                        "nonnegative_flux",
                        passed=False,
                        scope_key=scope_key,
                        observed={"minimum": min(severe_negative), "count": len(severe_negative)},
                        expected={"minimum": -tolerance},
                        default_severity="reject",
                    )
                )
            elif negative:
                row["values"] = [
                    0.0 if value is not None and float(value) < 0 else value for value in values
                ]
                warning = True
                row_findings.append(
                    _finding(
                        policy,
                        "negative_flux_clamped",
                        passed=False,
                        scope_key=scope_key,
                        observed={"minimum": min(negative), "count": len(negative)},
                        expected={"minimum": 0.0, "tolerance": tolerance},
                        default_severity="warning",
                    )
                )

        null_count = sum(value is None for value in values)
        null_ratio = null_count / len(values) if values else 1.0
        null_ratio_ok = null_ratio <= maximum_null_ratio
        row_findings.append(
            _finding(
                policy,
                "null_cell_ratio",
                passed=null_ratio_ok,
                scope_key=scope_key,
                observed={"null_count": null_count, "ratio": null_ratio},
                expected={"maximum_ratio": maximum_null_ratio},
                default_severity="warning",
            )
        )
        warning = warning or not null_ratio_ok
        findings.extend(row_findings)
        rejects = [
            finding
            for finding in row_findings
            if finding.severity == "reject" and finding.status in {"failed", "error"}
        ]
        if rejects:
            rejected.append(
                RejectedRecord(
                    source_record_key=scope_key,
                    record=row,
                    record_uri=None,
                    findings=tuple(rejects),
                )
            )
            continue
        row["quality_status"] = "warning" if warning else "passed"
        accepted.append(row)

    return QualityEvaluation.from_parts(
        accepted_rows=accepted,
        rejected_rows=rejected,
        findings=findings,
        input_row_count=len(rows),
    )
