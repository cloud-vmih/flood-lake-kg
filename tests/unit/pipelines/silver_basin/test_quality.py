from __future__ import annotations

from flashflood_data.pipelines.silver_basin.models import TopologyReport
from flashflood_data.shared.quality.policy import QualityPolicy, QualityRule


def _policy() -> QualityPolicy:
    severities = {
        "basin_business_key_unique": "fatal",
        "basin_geometry_valid": "fatal",
        "basin_area_positive": "fatal",
        "basin_topology_acyclic": "fatal",
        "basin_internal_target_exists": "fatal",
        "basin_source_id_present": "reject",
        "basin_atlas_joinable": "reject",
        "basin_geometry_repaired": "warning",
        "basin_area_consistent": "warning",
        "basin_sort_hint_consistent": "warning",
        "basin_sink_metadata_complete": "warning",
    }
    rules = tuple(
        QualityRule(
            rule_id=rule_id,
            rule_version="v1",
            severity=severity,
            parameters={"relative_tolerance": 0.05}
            if rule_id == "basin_area_consistent"
            else None,
        )
        for rule_id, severity in severities.items()
    )
    return QualityPolicy(
        policy_name="silver_basin_topology",
        policy_version="v1",
        layer="silver",
        rules=rules,
        defaults={},
        policy_id="policy-id",
    )


def _report(*, cycles: tuple[str, ...] = ()) -> TopologyReport:
    return TopologyReport(
        basin_count=2,
        edge_count=2,
        internal_edge_count=1,
        exit_edge_count=0,
        terminal_edge_count=1,
        topological_order=() if cycles else ("A", "B"),
        cycle_nodes=cycles,
    )


def test_quality_plans_cover_every_policy_rule_once() -> None:
    from flashflood_data.pipelines.silver_basin.quality import compile_basin_quality_plans

    plans = compile_basin_quality_plans(_policy(), _report())

    rules = plans.basin.rules + plans.edge.rules
    assert {rule.rule_id for rule in rules} == {
        rule.rule_id for rule in _policy().rules
    }
    assert len(rules) == len(_policy().rules)
    assert {rule.rule_id for rule in plans.basin.reject_rules} == {
        "basin_source_id_present",
        "basin_atlas_joinable",
    }


def test_cycle_report_compiles_to_failing_fatal_predicate() -> None:
    from flashflood_data.pipelines.silver_basin.quality import compile_basin_quality_plans

    plans = compile_basin_quality_plans(_policy(), _report(cycles=("A", "B")))
    rule = next(
        rule for rule in plans.edge.rules if rule.rule_id == "basin_topology_acyclic"
    )

    assert rule.severity == "fatal"
    assert rule.predicate_sql == "FALSE"


def test_quality_plan_rejects_unknown_or_missing_domain_rules() -> None:
    import pytest

    from flashflood_data.pipelines.silver_basin.quality import compile_basin_quality_plans

    policy = _policy()
    incomplete = QualityPolicy(
        policy_name=policy.policy_name,
        policy_version=policy.policy_version,
        layer=policy.layer,
        rules=policy.rules[:-1],
        defaults=policy.defaults,
        policy_id=policy.policy_id,
    )

    with pytest.raises(ValueError, match="exactly"):
        compile_basin_quality_plans(incomplete, _report())
