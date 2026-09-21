"""Versioned source and dataset registry rows for the static landing pipeline."""

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import yaml

from flashflood_data.catalog.models import SourceSpec


def load_static_registry(
    path: Path,
    specifications: Mapping[str, SourceSpec],
    *,
    catalog_name: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Join curated providers to actual source versions and declared dataset contracts."""
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    source_config = document["sources"]
    if set(source_config) != set(specifications):
        raise ValueError("Meta registry source IDs do not match source specifications")
    valid_from = datetime.fromisoformat(str(document["valid_from"])).astimezone(UTC)
    sources = [
        {
            "source_id": source_id,
            "source_version": str(spec.version),
            "provider": source_config[source_id]["provider"],
            "dataset": source_config[source_id]["dataset"],
            "license_uri": source_config[source_id].get("license_uri"),
            "coverage_ref": source_config[source_id].get("coverage_ref"),
            "refresh_sla_minutes": source_config[source_id].get("refresh_sla_minutes"),
            "valid_from": valid_from,
            "valid_to": None,
        }
        for source_id, spec in sorted(specifications.items())
    ]
    access_ref = "config/meta/static.yaml#policies.access.bootstrap-root-only"
    retention_ref = "config/meta/static.yaml#policies.retention.manual-review"
    if not document["policies"]["access"]["bootstrap-root-only"] or not document["policies"]["retention"]["manual-review"]:
        raise ValueError("Meta registry policies must be declared")
    datasets = [
        {
            "dataset_id": f"{catalog_name}.{name}",
            "contract_version": str(document["contract_version"]),
            "layer": name.split(".", 1)[0],
            "description": description,
            "owner": document["owner"],
            "source_id": None,
            "schema_ref": f"docs/schema_contract/data.md#{name}",
            "data_classification": "internal",
            "license_id": None,
            "retention_policy_ref": retention_ref,
            "freshness_sla_minutes": None,
            "quality_policy_id": "landing-verified-object-v1" if name.startswith("meta.") else "bronze-source-parse-v1",
            "access_policy_ref": access_ref,
            "valid_from": valid_from,
            "valid_to": None,
        }
        for name, description in sorted(document["datasets"].items())
    ]
    return sources, datasets
