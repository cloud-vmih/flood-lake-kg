from __future__ import annotations

from pathlib import Path

import pytest

from flashflood_data.shared.quality import load_quality_policy


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_policy_id_is_stable_when_yaml_key_order_changes(tmp_path: Path) -> None:
    left = _write(
        tmp_path / "left.yaml",
        """
policy_name: bronze_static
policy_version: v1
layer: bronze
rules:
  - rule_id: bbox_valid
    rule_version: v2
    severity: reject
    parameters:
      maximum: 180
      minimum: -180
""",
    )
    right = _write(
        tmp_path / "right.yaml",
        """
rules:
  - severity: reject
    parameters: {minimum: -180, maximum: 180}
    rule_version: v2
    rule_id: bbox_valid
layer: bronze
policy_version: v1
policy_name: bronze_static
""",
    )

    assert load_quality_policy(left).policy_id == load_quality_policy(right).policy_id


def test_policy_rejects_duplicate_rule_ids(tmp_path: Path) -> None:
    policy = _write(
        tmp_path / "duplicate.yaml",
        """
policy_name: bronze_static
policy_version: v1
layer: bronze
rules:
  - {rule_id: bbox_valid, rule_version: v1, severity: reject}
  - {rule_id: bbox_valid, rule_version: v2, severity: warning}
""",
    )

    with pytest.raises(ValueError, match="duplicate quality rule: bbox_valid"):
        load_quality_policy(policy)


def test_repository_quality_configs_load_with_expected_rules() -> None:
    root = Path(__file__).parents[4]
    raw = load_quality_policy(root / "config/quality/raw_landing.yaml")
    static = load_quality_policy(root / "config/quality/bronze_static.yaml")
    weather = load_quality_policy(root / "config/quality/bronze_weather.yaml")
    silver = load_quality_policy(root / "config/quality/silver_defaults.yaml")
    gold = load_quality_policy(root / "config/quality/gold_defaults.yaml")

    assert {rule.rule_id for rule in raw.rules} == {
        "object_checksum",
        "object_size",
        "object_readable",
    }

    assert {rule.rule_id for rule in static.rules} >= {
        "object_checksum",
        "object_readable",
        "crs_usable",
        "nonempty_parse",
        "business_key_unique",
        "bbox_valid",
        "geometry_valid",
    }
    assert {rule.rule_id for rule in weather.rules} >= {
        "parallel_array_lengths",
        "source_grid_complete",
        "unit_present",
        "temporal_window_valid",
        "nonnegative_flux",
        "null_cell_ratio",
    }
    assert silver.layer == "silver" and silver.rules == ()
    assert gold.layer == "gold" and gold.rules == ()
