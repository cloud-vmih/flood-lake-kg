from __future__ import annotations

import sys
from datetime import UTC, datetime
from types import ModuleType

import pytest

from flashflood_data.shared.quality.models import QualityFinding


def test_quality_plan_partitions_rules_and_builds_deterministic_rejection_id() -> None:
    from flashflood_data.shared.spark.quality import (
        SparkQualityRule,
        compile_quality_plan,
        rejection_id,
    )

    rules = (
        SparkQualityRule("geometry_valid", "v1", "fatal", "geometry_valid"),
        SparkQualityRule("area_positive", "v1", "reject", "area_km2 > 0"),
        SparkQualityRule("sink_present", "v1", "warning", "sink_id IS NOT NULL"),
    )
    plan = compile_quality_plan(rules)

    assert [rule.rule_id for rule in plan.fatal_rules] == ["geometry_valid"]
    assert [rule.rule_id for rule in plan.reject_rules] == ["area_positive"]
    assert [rule.rule_id for rule in plan.warning_rules] == ["sink_present"]
    assert rejection_id("run-1", "silver.dim_basin", "42", ("b", "a")) == rejection_id(
        "run-1", "silver.dim_basin", "42", ("a", "b")
    )
    assert rejection_id("run-1", "silver.dim_basin", "42", ("a",)) == rejection_id(
        "run-2", "silver.dim_basin", "42", ("a",)
    )


def test_quality_rule_rejects_unsafe_or_duplicate_configuration() -> None:
    from flashflood_data.shared.spark.quality import SparkQualityRule, compile_quality_plan

    with pytest.raises(ValueError, match="predicate"):
        SparkQualityRule("bad", "v1", "reject", "x > 0; DROP TABLE silver.x")
    rule = SparkQualityRule("same", "v1", "reject", "x > 0")
    with pytest.raises(ValueError, match="duplicate"):
        compile_quality_plan((rule, rule))


def test_quality_findings_are_serialized_for_meta_in_one_batch() -> None:
    from flashflood_data.shared.spark.quality import quality_rows

    checked_at = datetime(2026, 1, 1, tzinfo=UTC)
    rows = quality_rows(
        (
            QualityFinding(
                rule_id="area_positive",
                rule_version="v2",
                status="failed",
                severity="reject",
                scope_key="silver.dim_basin",
                observed_value={"failed_row_count": 2},
                expected_value={"minimum": 0},
                failed_row_count=2,
            ),
        ),
        pipeline_run_id="run-1",
        dataset_id="silver.dim_basin",
        checked_at=checked_at,
    )

    assert rows[0]["pipeline_run_id"] == "run-1"
    assert rows[0]["check_phase"] == "pre_publish"
    assert rows[0]["observed_value_json"] == '{"failed_row_count":2}'
    assert rows[0]["checked_at"] is checked_at


def test_quarantine_projection_matches_the_rejected_rows_contract() -> None:
    from flashflood_data.shared.spark.quality import quarantine_projection

    projection = quarantine_projection(
        (
            "basin_id",
            "basin_version",
            "_dq_rejection_id",
            "_dq_rule_ids",
            "_dq_source_record_key",
        ),
        pipeline_run_id="run-1",
        target_dataset_id="silver.dim_basin",
        quality_policy_id="basin-v1",
        input_table="bronze.basin_polygon_raw",
        input_snapshot_id=17,
        parser_version="normalize-v1",
        rejected_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert len(projection) == 15
    assert projection[0].endswith("AS rejection_id")
    assert "named_struct('basin_id', basin_id" in " ".join(projection)
    assert projection[-1].endswith("AS rejected_at")


def test_quality_evaluates_all_rule_counts_in_one_spark_aggregation(monkeypatch) -> None:
    from flashflood_data.shared.spark.quality import (
        SparkQualityRule,
        apply_quality_plan,
        compile_quality_plan,
    )

    class Expression:
        def __invert__(self):
            return self

        def __or__(self, _other):
            return self

        def otherwise(self, _value):
            return self

        def alias(self, name):
            self.name = name
            return self

    functions = ModuleType("pyspark.sql.functions")
    functions.expr = lambda _value: Expression()
    functions.lit = lambda _value: Expression()
    functions.coalesce = lambda *_values: Expression()
    functions.when = lambda *_values: Expression()
    functions.sum = lambda value: value
    sql = ModuleType("pyspark.sql")
    sql.functions = functions
    pyspark = ModuleType("pyspark")
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)

    class Result:
        def collect(self):
            return [{"present": 0, "plausible": 1}]

    class Frame:
        columns = ("basin_id",)

        def __init__(self):
            self.agg_calls = 0

        def agg(self, *_expressions):
            self.agg_calls += 1
            return Result()

        def filter(self, _expression):
            return self

    frame = Frame()
    outcome = apply_quality_plan(
        frame,
        compile_quality_plan(
            (
                SparkQualityRule("present", "v1", "info", "basin_id IS NOT NULL"),
                SparkQualityRule("plausible", "v1", "warning", "basin_id <> '0'"),
            )
        ),
        pipeline_run_id="run-1",
        dataset_id="silver.dim_basin",
        key_fields=("basin_id",),
    )

    assert frame.agg_calls == 1
    assert outcome.quality_status == "warning"
