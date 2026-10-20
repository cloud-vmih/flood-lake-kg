"""Domain DQ rules for canonical L12 basin rows and directed basin edges."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from flashflood_data.pipelines.silver_basin.models import TopologyReport
from flashflood_data.shared.quality.policy import QualityPolicy, QualityRule
from flashflood_data.shared.spark.quality import (
    SparkQualityOutcome,
    SparkQualityPlan,
    SparkQualityRule,
    apply_quality_plan,
    compile_quality_plan,
)

DIM_BASIN_COLUMNS = (
    "basin_id",
    "basin_version",
    "hydrobasins_level",
    "geometry_wkb",
    "bbox_wgs84",
    "crs",
    "geometry_hash",
    "next_down_id",
    "main_basin_id",
    "pfaf_id",
    "topology_sort_hint",
    "hydro_order",
    "is_coastal",
    "is_endorheic",
    "basin_area_km2",
    "upstream_area_km2",
    "distance_to_main_sink_km",
    "next_sink_id",
    "distance_to_sink_km",
    "valid_from",
    "valid_to",
    "hydrobasins_object_id",
    "basinatlas_object_id",
    "quality_status",
)

BASIN_EDGE_COLUMNS = (
    "basin_edge_id",
    "topology_version",
    "basin_version",
    "upstream_basin_id",
    "downstream_basin_id",
    "source_downstream_id",
    "edge_status",
    "topological_rank_upstream_first",
    "terminal_type",
    "cycle_check",
    "outlet_check",
    "quality_status",
    "source_object_id",
)

_BASIN_RULES = {
    "basin_business_key_unique",
    "basin_geometry_valid",
    "basin_area_positive",
    "basin_source_id_present",
    "basin_atlas_joinable",
    "basin_geometry_repaired",
    "basin_area_consistent",
    "basin_sink_metadata_complete",
}
_EDGE_RULES = {
    "basin_topology_acyclic",
    "basin_internal_target_exists",
    "basin_sort_hint_consistent",
}
_ALL_RULES = _BASIN_RULES | _EDGE_RULES


@dataclass(frozen=True)
class BasinQualityPlans:
    """Compiled row and graph plans derived from one versioned YAML policy."""

    basin: SparkQualityPlan
    edge: SparkQualityPlan


def _tolerance(rule: QualityRule) -> float:
    parameters = rule.parameters or {}
    value = parameters.get("relative_tolerance")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("basin_area_consistent requires numeric relative_tolerance")
    tolerance = float(value)
    if not 0 <= tolerance < 1:
        raise ValueError("basin area relative tolerance must be in [0, 1)")
    return tolerance


def _predicate(rule: QualityRule, report: TopologyReport) -> str:
    if rule.rule_id == "basin_business_key_unique":
        return "_dq_business_key_count = 1"
    if rule.rule_id == "basin_geometry_valid":
        return (
            "geometry_wkb IS NOT NULL AND "
            "ST_IsValid(ST_GeomFromWKB(geometry_wkb)) AND "
            "NOT ST_IsEmpty(ST_GeomFromWKB(geometry_wkb))"
        )
    if rule.rule_id == "basin_area_positive":
        return (
            "basin_area_km2 IS NULL OR upstream_area_km2 IS NULL OR "
            "(basin_area_km2 > 0 AND upstream_area_km2 > 0 "
            "AND upstream_area_km2 >= basin_area_km2)"
        )
    if rule.rule_id == "basin_source_id_present":
        return "hydrobasins_object_id IS NOT NULL AND trim(hydrobasins_object_id) <> ''"
    if rule.rule_id == "basin_atlas_joinable":
        return (
            "basinatlas_object_id IS NOT NULL AND basin_area_km2 IS NOT NULL "
            "AND upstream_area_km2 IS NOT NULL"
        )
    if rule.rule_id == "basin_geometry_repaired":
        return "_geometry_was_repaired = FALSE"
    if rule.rule_id == "basin_area_consistent":
        tolerance = _tolerance(rule)
        computed = (
            "(ST_Area(ST_Transform(ST_GeomFromWKB(geometry_wkb), "
            "'EPSG:4326', 'EPSG:6933')) / 1000000.0)"
        )
        return (
            f"abs({computed} - basin_area_km2) / "
            f"greatest(basin_area_km2, 0.000001) <= {tolerance}"
        )
    if rule.rule_id == "basin_sink_metadata_complete":
        return "next_sink_id IS NOT NULL AND distance_to_sink_km IS NOT NULL"
    if rule.rule_id == "basin_topology_acyclic":
        return "TRUE" if not report.cycle_nodes else "FALSE"
    if rule.rule_id == "basin_internal_target_exists":
        return "edge_status <> 'internal' OR downstream_basin_id IS NOT NULL"
    if rule.rule_id == "basin_sort_hint_consistent":
        return "_sort_hint_consistent = TRUE"
    raise ValueError(f"unsupported basin quality rule: {rule.rule_id}")


def _spark_rule(rule: QualityRule, report: TopologyReport) -> SparkQualityRule:
    return SparkQualityRule(
        rule_id=rule.rule_id,
        rule_version=rule.rule_version,
        severity=rule.severity,
        predicate_sql=_predicate(rule, report),
    )


def compile_basin_quality_plans(
    policy: QualityPolicy,
    report: TopologyReport,
) -> BasinQualityPlans:
    """Compile the complete approved policy; silently omitted rules are forbidden."""
    if policy.layer != "silver" or policy.policy_name != "silver_basin_topology":
        raise ValueError("basin topology requires its Silver quality policy")
    enabled = tuple(rule for rule in policy.rules if rule.enabled)
    rule_ids = {rule.rule_id for rule in enabled}
    if rule_ids != _ALL_RULES or len(enabled) != len(_ALL_RULES):
        raise ValueError("basin topology policy must contain exactly the approved domain rules")
    return BasinQualityPlans(
        basin=compile_quality_plan(
            tuple(_spark_rule(rule, report) for rule in enabled if rule.rule_id in _BASIN_RULES)
        ),
        edge=compile_quality_plan(
            tuple(_spark_rule(rule, report) for rule in enabled if rule.rule_id in _EDGE_RULES)
        ),
    )


def prepare_basin_quality_frame(frame: Any) -> Any:
    """Add only the transient duplicate-count evidence required by basin DQ."""
    from pyspark.sql import Window
    from pyspark.sql import functions as spark_functions

    key_window = Window.partitionBy("basin_id", "basin_version")
    return frame.withColumn(
        "_dq_business_key_count",
        spark_functions.count(spark_functions.lit(1)).over(key_window),
    )


def evaluate_basin_quality(
    frame: Any,
    plan: SparkQualityPlan,
    *,
    pipeline_run_id: str,
) -> SparkQualityOutcome:
    return apply_quality_plan(
        prepare_basin_quality_frame(frame),
        plan,
        pipeline_run_id=pipeline_run_id,
        dataset_id="silver.dim_basin",
        key_fields=("basin_id", "basin_version"),
    )


def evaluate_edge_quality(
    frame: Any,
    plan: SparkQualityPlan,
    *,
    pipeline_run_id: str,
) -> SparkQualityOutcome:
    return apply_quality_plan(
        frame,
        plan,
        pipeline_run_id=pipeline_run_id,
        dataset_id="silver.basin_edge",
        key_fields=("topology_version", "basin_version", "upstream_basin_id"),
    )


def publishable_frame(frame: Any, columns: tuple[str, ...], quality_status: str) -> Any:
    """Replace staging status and project the exact physical Iceberg contract."""
    from pyspark.sql import functions as spark_functions

    if quality_status not in {"passed", "warning"}:
        raise ValueError("publishable basin rows require passed or warning status")
    return frame.withColumn(
        "quality_status", spark_functions.lit(quality_status)
    ).select(*columns)
