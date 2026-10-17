"""Static Bronze object and feature rules.

Parsers preserve source evidence. This module owns the decision to publish a
feature, publish it with a warning, or send it to quarantine.
"""

import math
from collections import Counter
from collections.abc import Mapping, Sequence

from shapely import from_wkb
from shapely.errors import GEOSException

from flashflood_data.shared.quality import (
    QualityEvaluation,
    QualityFinding,
    QualityPolicy,
    QualityResult,
    QualityRule,
    RejectedRecord,
)
from flashflood_data.storage.iceberg_schemas import BRONZE_KEYS

_GEOMETRY_TABLES = frozenset(
    {"basin_polygon_raw", "river_reach_raw", "admin_boundary_raw", "osm_feature_raw"}
)


def default_static_quality_policy() -> QualityPolicy:
    """Return the in-code fallback used by isolated tests and library callers."""
    severities = {
        "object_checksum": "fatal",
        "object_readable": "fatal",
        "crs_usable": "fatal",
        "nonempty_parse": "fatal",
        "business_key_unique": "reject",
        "bbox_valid": "reject",
        "geometry_valid": "reject",
        "geometry_repaired": "warning",
        "bbox_clamped": "warning",
        "optional_source_field_missing": "warning",
    }
    return QualityPolicy(
        policy_name="bronze_static",
        policy_version="v1",
        layer="bronze",
        rules=tuple(
            QualityRule(rule_id=rule_id, rule_version="v1", severity=severity)  # type: ignore[arg-type]
            for rule_id, severity in severities.items()
        ),
        defaults={"published_quality_status": ["passed", "warning"]},
        policy_id="bronze_static:v1:built-in",
    )


def _valid_bbox(value: object) -> bool:
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return False
    try:
        west, south, east, north = (float(item) for item in value)
    except (TypeError, ValueError):
        return False
    return (
        all(math.isfinite(item) for item in (west, south, east, north))
        and -180 <= west <= east <= 180
        and -90 <= south <= north <= 90
    )


def check_parsed_rows(table_name: str, rows: Sequence[Mapping[str, object]]) -> list[QualityResult]:
    """Check source-grain uniqueness and spatial envelope before Iceberg commit."""
    keys = BRONZE_KEYS[table_name]
    identities = [tuple(row.get(name) for name in keys) for row in rows]
    duplicate_count = len(identities) - len(set(identities))
    bad_bbox = sum(
        1
        for row in rows
        if row.get("bbox_wgs84") is not None and not _valid_bbox(row["bbox_wgs84"])
    )
    return [
        QualityResult(
            "nonempty_parse",
            "passed" if rows else "failed",
            "fatal",
            {"row_count": len(rows)},
            0 if rows else 1,
        ),
        QualityResult(
            "business_key_unique",
            "passed" if duplicate_count == 0 else "failed",
            "fatal",
            {"duplicate_count": duplicate_count},
            duplicate_count,
        ),
        QualityResult(
            "bbox_valid",
            "passed" if bad_bbox == 0 else "failed",
            "fatal",
            {"invalid_count": bad_bbox},
            bad_bbox,
        ),
    ]


def _policy_rule(
    policy: QualityPolicy,
    rule_id: str,
    *,
    default_severity: str,
) -> tuple[str, str]:
    rule = next((item for item in policy.rules if item.rule_id == rule_id and item.enabled), None)
    if rule is None:
        return "v1", default_severity
    return rule.rule_version, rule.severity


def _finding(
    policy: QualityPolicy,
    rule_id: str,
    status: str,
    observed_value: object,
    *,
    scope_key: str,
    default_severity: str,
    failed_row_count: int = 0,
    expected_value: object | None = None,
) -> QualityFinding:
    version, severity = _policy_rule(
        policy, rule_id, default_severity=default_severity
    )
    return QualityFinding(
        rule_id=rule_id,
        rule_version=version,
        status=status,  # type: ignore[arg-type]
        severity=severity,  # type: ignore[arg-type]
        scope_key=scope_key,
        observed_value=observed_value,
        expected_value=expected_value,
        failed_row_count=failed_row_count,
    )


def _business_identity(
    table_name: str, row: Mapping[str, object]
) -> tuple[object, ...] | None:
    values = tuple(row.get(name) for name in BRONZE_KEYS[table_name])
    return None if any(value is None or str(value).strip() == "" for value in values) else values


def _source_record_key(
    table_name: str, row: Mapping[str, object], position: int
) -> str:
    explicit = row.get("_dq_source_record_key")
    if explicit is not None and str(explicit):
        return str(explicit)
    identity = _business_identity(table_name, row)
    if identity is not None:
        return "|".join(map(str, identity))
    return f"row-{position}"


def _geometry_is_valid(table_name: str, row: Mapping[str, object]) -> bool:
    if table_name not in _GEOMETRY_TABLES:
        return True
    payload = row.get("geometry_wkb")
    if not isinstance(payload, (bytes, bytearray, memoryview)) or not payload:
        return False
    try:
        geometry = from_wkb(bytes(payload))
    except (GEOSException, TypeError, ValueError):
        return False
    if geometry.is_empty or not geometry.is_valid:
        return False
    if table_name in {"basin_polygon_raw", "admin_boundary_raw"}:
        return geometry.geom_type in {"Polygon", "MultiPolygon"}
    if table_name == "river_reach_raw":
        return geometry.geom_type in {"LineString", "MultiLineString"}
    return True


def evaluate_static_object(
    table_name: str,
    object_context: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
    policy: QualityPolicy,
    *,
    require_accepted: bool = True,
) -> QualityEvaluation:
    """Partition one parsed static object according to the versioned policy."""
    if table_name not in BRONZE_KEYS:
        raise ValueError(f"unsupported static Bronze table: {table_name}")
    object_id = str(object_context.get("object_id") or "object")
    findings: list[QualityFinding] = []
    object_rules = (
        ("object_checksum", "checksum_valid"),
        ("object_readable", "readable"),
        ("crs_usable", "crs_usable"),
    )
    for rule_id, context_key in object_rules:
        valid = bool(object_context.get(context_key, True))
        findings.append(
            _finding(
                policy,
                rule_id,
                "passed" if valid else "failed",
                {context_key: valid},
                scope_key=object_id,
                default_severity="fatal",
                failed_row_count=0 if valid else 1,
                expected_value={context_key: True},
            )
        )
    findings.append(
        _finding(
            policy,
            "nonempty_parse",
            "passed" if rows else "failed",
            {"row_count": len(rows)},
            scope_key=object_id,
            default_severity="fatal",
            failed_row_count=0 if rows else 1,
            expected_value={"minimum_row_count": 1},
        )
    )

    identities = [_business_identity(table_name, row) for row in rows]
    duplicate_identities = {
        identity for identity, count in Counter(identity for identity in identities if identity).items()
        if count > 1
    }
    accepted: list[Mapping[str, object]] = []
    rejected: list[RejectedRecord] = []
    for position, source_row in enumerate(rows, start=1):
        row = dict(source_row)
        scope_key = _source_record_key(table_name, row, position)
        row_findings: list[QualityFinding] = []
        identity = identities[position - 1]
        duplicate = identity in duplicate_identities or bool(
            row.get("_dq_duplicate_business_key")
        )
        if identity is None or duplicate:
            row_findings.append(
                _finding(
                    policy,
                    "business_key_unique",
                    "failed",
                    {"identity": identity, "duplicate": duplicate},
                    scope_key=scope_key,
                    default_severity="reject",
                    failed_row_count=1,
                    expected_value={"present": True, "unique": True},
                )
            )
        if not _valid_bbox(row.get("bbox_wgs84")):
            row_findings.append(
                _finding(
                    policy,
                    "bbox_valid",
                    "failed",
                    {"bbox_wgs84": row.get("bbox_wgs84")},
                    scope_key=scope_key,
                    default_severity="reject",
                    failed_row_count=1,
                )
            )
        if not _geometry_is_valid(table_name, row):
            row_findings.append(
                _finding(
                    policy,
                    "geometry_valid",
                    "failed",
                    {"valid": False},
                    scope_key=scope_key,
                    default_severity="reject",
                    failed_row_count=1,
                    expected_value={"valid": True},
                )
            )
        for warning_code in tuple(row.get("_dq_warning_codes") or ()):
            row_findings.append(
                _finding(
                    policy,
                    str(warning_code),
                    "failed",
                    {"warning": str(warning_code)},
                    scope_key=scope_key,
                    default_severity="warning",
                    failed_row_count=1,
                )
            )
        findings.extend(row_findings)
        failed_rejects = [
            item
            for item in row_findings
            if item.severity == "reject" and item.status in {"failed", "error"}
        ]
        if failed_rejects:
            rejected.append(
                RejectedRecord(
                    source_record_key=scope_key,
                    record={key: value for key, value in row.items() if not key.startswith("_dq_")},
                    record_uri=None,
                    findings=tuple(failed_rejects),
                )
            )
            continue
        clean = {key: value for key, value in row.items() if not key.startswith("_dq_")}
        clean["quality_status"] = (
            "warning"
            if any(
                item.severity == "warning" and item.status in {"failed", "error"}
                for item in row_findings
            )
            else "passed"
        )
        accepted.append(clean)

    return QualityEvaluation.from_parts(
        accepted_rows=accepted,
        rejected_rows=rejected,
        findings=findings,
        input_row_count=len(rows) if require_accepted else 0,
    )
