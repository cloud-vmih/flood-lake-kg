"""Versioned, content-addressed quality-policy loader."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml

from flashflood_data.shared.quality.models import Severity

Layer = Literal["raw", "bronze", "silver", "gold"]


@dataclass(frozen=True)
class QualityRule:
    rule_id: str
    rule_version: str
    severity: Severity
    enabled: bool = True
    parameters: Mapping[str, object] | None = None


@dataclass(frozen=True)
class QualityPolicy:
    policy_name: str
    policy_version: str
    layer: Layer
    rules: tuple[QualityRule, ...]
    defaults: Mapping[str, object]
    policy_id: str


def _required_string(data: Mapping[str, object], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"quality policy {field} must be a non-empty string")
    return value.strip()


def _canonical_payload(data: Mapping[str, object]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def load_quality_policy(path: Path) -> QualityPolicy:
    """Load, validate, and fingerprint one YAML quality policy."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("quality policy root must be a mapping")

    policy_name = _required_string(raw, "policy_name")
    policy_version = _required_string(raw, "policy_version")
    layer = _required_string(raw, "layer")
    if layer not in {"raw", "bronze", "silver", "gold"}:
        raise ValueError(f"unsupported quality policy layer: {layer}")

    raw_rules = raw.get("rules", [])
    if not isinstance(raw_rules, list):
        raise TypeError("quality policy rules must be a list")
    rules: list[QualityRule] = []
    seen: set[str] = set()
    for raw_rule in raw_rules:
        if not isinstance(raw_rule, dict):
            raise TypeError("each quality rule must be a mapping")
        rule_id = _required_string(raw_rule, "rule_id")
        if rule_id in seen:
            raise ValueError(f"duplicate quality rule: {rule_id}")
        seen.add(rule_id)
        severity = _required_string(raw_rule, "severity")
        if severity not in {"fatal", "reject", "warning", "info"}:
            raise ValueError(f"unsupported severity for {rule_id}: {severity}")
        parameters = raw_rule.get("parameters")
        if parameters is not None and not isinstance(parameters, dict):
            raise ValueError(f"quality rule parameters must be a mapping: {rule_id}")
        rules.append(
            QualityRule(
                rule_id=rule_id,
                rule_version=_required_string(raw_rule, "rule_version"),
                severity=severity,  # type: ignore[arg-type]
                enabled=bool(raw_rule.get("enabled", True)),
                parameters=parameters,
            )
        )

    defaults = raw.get("defaults", {})
    if not isinstance(defaults, dict):
        raise TypeError("quality policy defaults must be a mapping")
    canonical = _canonical_payload(raw)
    return QualityPolicy(
        policy_name=policy_name,
        policy_version=policy_version,
        layer=layer,  # type: ignore[arg-type]
        rules=tuple(rules),
        defaults=defaults,
        policy_id=f"{policy_name}:{policy_version}:{hashlib.sha256(canonical.encode()).hexdigest()}",
    )
