"""Shared Spark DQ planning and row partitioning for Silver applications."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from typing import Any, Literal

from flashflood_data.shared.quality.models import QualityFinding

_UTC = timezone.utc  # noqa: UP017 - Spark image currently uses Python 3.10

SparkSeverity = Literal["fatal", "reject", "warning", "info"]
_RULE_ID = re.compile(r"^[a-z][a-z0-9_]*$")
_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNSAFE_SQL = (";", "--", "/*", "*/")


@dataclass(frozen=True)
class SparkQualityRule:
    rule_id: str
    rule_version: str
    severity: SparkSeverity
    predicate_sql: str

    def __post_init__(self) -> None:
        if _RULE_ID.fullmatch(self.rule_id) is None or not self.rule_version:
            raise ValueError("Spark quality rule identity is invalid")
        if self.severity not in {"fatal", "reject", "warning", "info"}:
            raise ValueError(f"unsupported Spark quality severity: {self.severity}")
        if not self.predicate_sql.strip() or any(
            marker in self.predicate_sql for marker in _UNSAFE_SQL
        ):
            raise ValueError("Spark quality predicate contains unsafe SQL")


@dataclass(frozen=True)
class SparkQualityPlan:
    rules: tuple[SparkQualityRule, ...]
    fatal_rules: tuple[SparkQualityRule, ...]
    reject_rules: tuple[SparkQualityRule, ...]
    warning_rules: tuple[SparkQualityRule, ...]
    info_rules: tuple[SparkQualityRule, ...]


@dataclass(frozen=True)
class SparkQualityOutcome:
    accepted: Any
    rejected: Any
    findings: tuple[QualityFinding, ...]
    blocked: bool
    quality_status: Literal["passed", "warning", "failed"]


def compile_quality_plan(rules: Sequence[SparkQualityRule]) -> SparkQualityPlan:
    collected = tuple(rules)
    identities = [(rule.rule_id, rule.rule_version) for rule in collected]
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate Spark quality rule identity")
    return SparkQualityPlan(
        rules=collected,
        fatal_rules=tuple(rule for rule in collected if rule.severity == "fatal"),
        reject_rules=tuple(rule for rule in collected if rule.severity == "reject"),
        warning_rules=tuple(rule for rule in collected if rule.severity == "warning"),
        info_rules=tuple(rule for rule in collected if rule.severity == "info"),
    )


def rejection_id(
    pipeline_run_id: str,
    dataset_id: str,
    source_record_key: str,
    rule_ids: Sequence[str],
) -> str:
    if not pipeline_run_id or not dataset_id or not source_record_key or not rule_ids:
        raise ValueError("rejection identity requires run, dataset, record key, and rules")
    # Run ID is stored as audit evidence but deliberately excluded from the
    # logical identity so a repair run upserts the same rejection.
    del pipeline_run_id
    payload = {
        "dataset_id": dataset_id,
        "source_record_key": source_record_key,
        "rule_ids": sorted(set(rule_ids)),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def quality_rows(
    findings: Sequence[QualityFinding],
    *,
    pipeline_run_id: str,
    dataset_id: str,
    checked_at: datetime,
    check_phase: str = "pre_publish",
    snapshot_table: str | None = None,
    snapshot_id: int | None = None,
) -> tuple[dict[str, object], ...]:
    """Serialize DQ findings into the complete Meta quality-results contract."""
    if not pipeline_run_id or not dataset_id:
        raise ValueError("quality rows require pipeline run and dataset identity")
    if check_phase not in {"pre_commit", "post_commit", "pre_publish"}:
        raise ValueError("unsupported Spark quality check phase")
    if checked_at.tzinfo is None or checked_at.utcoffset() != _UTC.utcoffset(checked_at):
        raise ValueError("quality checked_at must use timezone-aware UTC")
    if (snapshot_table is None) != (snapshot_id is None):
        raise ValueError("quality snapshot table and ID must be set together")
    return tuple(
        {
            "pipeline_run_id": pipeline_run_id,
            "check_phase": check_phase,
            "dataset_id": dataset_id,
            "rule_id": finding.rule_id,
            "rule_version": finding.rule_version,
            "scope_key": finding.scope_key or dataset_id,
            "severity": finding.severity,
            "status": finding.status,
            "observed_value_json": json.dumps(
                finding.observed_value, sort_keys=True, separators=(",", ":")
            ),
            "expected_value_json": (
                None
                if finding.expected_value is None
                else json.dumps(
                    finding.expected_value, sort_keys=True, separators=(",", ":")
                )
            ),
            "failed_row_count": finding.failed_row_count,
            "sample_uri": None,
            "snapshot_table": snapshot_table,
            "snapshot_id": snapshot_id,
            "checked_at": checked_at,
        }
        for finding in findings
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quarantine_projection(
    columns: Sequence[str],
    *,
    pipeline_run_id: str,
    target_dataset_id: str,
    quality_policy_id: str,
    input_table: str,
    input_snapshot_id: int,
    parser_version: str,
    rejected_at: datetime,
) -> tuple[str, ...]:
    """Build the exact ``quarantine.rejected_rows`` Spark projection."""
    names = tuple(columns)
    if any(_COLUMN.fullmatch(name) is None for name in names):
        raise ValueError("quarantine input contains an unsafe column name")
    required = {"_dq_rejection_id", "_dq_rule_ids", "_dq_source_record_key"}
    if not required.issubset(names):
        raise ValueError("quarantine input is missing DQ identity columns")
    if input_snapshot_id < 1:
        raise ValueError("quarantine input snapshot ID must be positive")
    if rejected_at.tzinfo is None or rejected_at.utcoffset() != _UTC.utcoffset(rejected_at):
        raise ValueError("quarantine rejected_at must use timezone-aware UTC")
    identities = (
        pipeline_run_id,
        target_dataset_id,
        quality_policy_id,
        input_table,
        parser_version,
    )
    if any(not value for value in identities):
        raise ValueError("quarantine identity fields must not be empty")

    record_columns = tuple(name for name in names if not name.startswith("_dq_"))
    if not record_columns:
        raise ValueError("quarantine record requires at least one business column")
    named_fields = ", ".join(
        value
        for name in record_columns
        for value in (_sql_literal(name), name)
    )
    timestamp = rejected_at.astimezone(_UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    return (
        "_dq_rejection_id AS rejection_id",
        f"{_sql_literal(pipeline_run_id)} AS pipeline_run_id",
        "'silver' AS layer",
        f"{_sql_literal(target_dataset_id)} AS target_dataset_id",
        "CAST(NULL AS STRING) AS object_id",
        f"{_sql_literal(input_table)} AS input_table",
        f"CAST({input_snapshot_id} AS BIGINT) AS input_snapshot_id",
        "_dq_source_record_key AS source_record_key",
        f"{_sql_literal(quality_policy_id)} AS quality_policy_id",
        "_dq_rule_ids AS rule_ids",
        "to_json(_dq_rule_ids) AS reason_codes_json",
        f"to_json(named_struct({named_fields})) AS record_json",
        "CAST(NULL AS STRING) AS record_uri",
        f"{_sql_literal(parser_version)} AS parser_version",
        f"TIMESTAMP {_sql_literal(timestamp)} AS rejected_at",
    )


def build_quarantine_frame(rejected: Any, **kwargs: Any) -> Any:
    """Project a rejected DataFrame to the shared quarantine Iceberg schema."""
    return rejected.selectExpr(quarantine_projection(rejected.columns, **kwargs))


def _failure(functions: Any, rule: SparkQualityRule) -> Any:
    return ~functions.coalesce(functions.expr(rule.predicate_sql), functions.lit(False))


def apply_quality_plan(
    frame: Any,
    plan: SparkQualityPlan,
    *,
    pipeline_run_id: str,
    dataset_id: str,
    key_fields: Sequence[str],
) -> SparkQualityOutcome:
    """Evaluate rules and return lazy accepted/rejected frames plus small findings."""
    from pyspark.sql import functions as spark_functions

    keys = tuple(key_fields)
    if not keys or any(key not in frame.columns for key in keys):
        raise ValueError("Spark quality key fields must be present and non-empty")
    findings: list[QualityFinding] = []
    failed_counts: dict[str, int] = {}
    if plan.rules:
        counts = frame.agg(
            *[
                spark_functions.sum(
                    spark_functions.when(
                        _failure(spark_functions, rule), spark_functions.lit(1)
                    ).otherwise(spark_functions.lit(0))
                ).alias(rule.rule_id)
                for rule in plan.rules
            ]
        ).collect()
        if not counts:
            raise RuntimeError("Spark quality aggregation returned no result")
        failed_counts = {
            rule.rule_id: int(counts[0][rule.rule_id]) for rule in plan.rules
        }
    for rule in plan.rules:
        count = failed_counts[rule.rule_id]
        findings.append(
            QualityFinding(
                rule_id=rule.rule_id,
                rule_version=rule.rule_version,
                status="passed" if count == 0 else "failed",
                severity=rule.severity,
                scope_key=dataset_id,
                observed_value={"failed_row_count": count},
                expected_value={"predicate": rule.predicate_sql},
                failed_row_count=count,
            )
        )

    blocked = any(failed_counts[rule.rule_id] for rule in plan.fatal_rules)
    if blocked:
        return SparkQualityOutcome(
            accepted=frame.limit(0),
            rejected=frame.limit(0),
            findings=tuple(findings),
            blocked=True,
            quality_status="failed",
        )

    reject_failures = [_failure(spark_functions, rule) for rule in plan.reject_rules]
    reject_condition = (
        reduce(lambda left, right: left | right, reject_failures)
        if reject_failures
        else spark_functions.lit(False)
    )
    accepted = frame.filter(~reject_condition)
    rejected = frame.filter(reject_condition)

    if plan.reject_rules:
        failed_rule_ids = spark_functions.array_compact(
            spark_functions.array(
                *[
                    spark_functions.when(
                        _failure(spark_functions, rule), spark_functions.lit(rule.rule_id)
                    )
                    for rule in plan.reject_rules
                ]
            )
        )
        source_record_key = spark_functions.concat_ws(
            "|", *[spark_functions.col(name).cast("string") for name in keys]
        )
        rejected = (
            rejected.withColumn("_dq_source_record_key", source_record_key)
            .withColumn("_dq_rule_ids", failed_rule_ids)
            .withColumn(
                "_dq_rejection_id",
                spark_functions.sha2(
                    spark_functions.concat_ws(
                        "\u001f",
                        spark_functions.lit(dataset_id),
                        spark_functions.col("_dq_source_record_key"),
                        spark_functions.array_join(
                            spark_functions.array_sort(
                                spark_functions.col("_dq_rule_ids")
                            ),
                            ",",
                        ),
                    ),
                    256,
                ),
            )
        )
    warning_found = any(
        failed_counts[rule.rule_id] > 0 for rule in plan.warning_rules
    )
    return SparkQualityOutcome(
        accepted=accepted,
        rejected=rejected,
        findings=tuple(findings),
        blocked=False,
        quality_status="warning" if warning_found else "passed",
    )
