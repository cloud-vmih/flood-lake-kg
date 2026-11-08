"""Weather-specific data-quality rules keep bad values out of Silver."""

from pathlib import Path

from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config
from flashflood_data.pipelines.silver_weather.quality import (
    weather_quality_plan,
    weight_quality_plan,
)

CONFIG = load_weather_aggregation_config(
    Path(__file__).resolve().parents[4] / "config/pipelines/silver/weather_aggregation.yaml"
)


def test_weather_quality_covers_identity_coverage_units_and_physical_ranges() -> None:
    plan = weather_quality_plan(CONFIG)
    rules = {rule.rule_id: rule for rule in plan.rules}
    assert rules["weather_identity"].severity == "fatal"
    assert rules["weather_coverage"].severity == "fatal"
    assert rules["weather_unit_kind"].severity == "fatal"
    assert rules["weather_physical_range"].severity == "reject"
    assert rules["weather_low_coverage"].severity == "warning"
    assert "value IS NULL" in rules["weather_physical_range"].predicate_sql
    assert "NOT isnan(value)" in rules["weather_physical_range"].predicate_sql
    assert "CAST('Infinity' AS DOUBLE)" in rules["weather_physical_range"].predicate_sql


def test_weight_quality_requires_positive_area_and_bounded_weights() -> None:
    plan = weight_quality_plan()
    rules = {rule.rule_id: rule for rule in plan.rules}
    assert rules["weight_area"].severity == "fatal"
    assert rules["weight_fraction"].severity == "fatal"
