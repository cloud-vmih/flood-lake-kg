"""Static quality checks split by subject."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from flashflood_data.catalog.models import AssetKind, AssetRecord
from flashflood_data.core.config import StudyAreaConfig
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.qa.models import CheckResult
from flashflood_data.static.qa.shared import (
    _asset_records,
    _check,
)

_GIB = 2**30


def _provenance_checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    assets = _asset_records(paths)
    by_id = {asset.asset_id: asset for asset in assets}

    def dependency_ids(asset: AssetRecord) -> set[str] | None:
        try:
            metadata = json.loads(asset.metadata_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(metadata, dict):
            return None
        references: set[str] = set()
        for key in ("source_asset_ids", "dependency_asset_ids", "input_asset_ids"):
            value = metadata.get(key, [])
            if value is None:
                continue
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                return None
            references.update(value)
        return references

    roots = [
        asset
        for asset in assets
        if asset.kind is AssetKind.DERIVED
        and (asset.asset_id.startswith("task15-") or asset.asset_id.startswith("task16-"))
    ]
    unresolved: set[str] = set()
    used_ids: set[str] = set()

    def visit(asset_id: str, visiting: set[str]) -> None:
        if asset_id in visiting:
            unresolved.add(asset_id)
            return
        asset = by_id.get(asset_id)
        if asset is None:
            unresolved.add(asset_id)
            return
        if asset.kind is AssetKind.RAW:
            used_ids.add(asset_id)
            return
        references = dependency_ids(asset)
        if not references:
            unresolved.add(asset_id)
            return
        for reference in sorted(references):
            visit(reference, visiting | {asset_id})

    for root in sorted(roots, key=lambda asset: asset.asset_id):
        visit(root.asset_id, set())
    raw_root = paths.raw.resolve()
    pipeline_raw = [
        asset
        for asset in assets
        if asset.kind is AssetKind.RAW
        and Path(asset.storage_path).resolve().is_relative_to(raw_root)
        and asset.duplicate_of_asset_id is None
    ]
    used = [asset for asset in assets if asset.kind is AssetKind.RAW and asset.asset_id in used_ids]
    required = ("source_uri", "source_version", "license_id", "checksum", "retrieved_at")
    incomplete = [
        asset.asset_id for asset in used if any(not getattr(asset, field) for field in required)
    ]
    raw_bytes = sum(asset.size_bytes for asset in pipeline_raw)
    disk_free = shutil.disk_usage(paths.dataset if paths.dataset.exists() else paths.root).free
    return [
        _check(
            "raw.provenance",
            bool(used) and not incomplete and not unresolved,
            "fatal",
            "every used raw asset has URI/version/license/retrieval/checksum",
            f"{len(used) - len(incomplete)}/{len(used)} used raw assets complete; {len(unresolved)} unresolved dependencies",
            "provenance for raw assets actually used through source-asset references",
            [*incomplete, *sorted(unresolved)],
        ),
        _check(
            "storage.raw_cap",
            raw_bytes <= int(config.new_raw_soft_cap_gib * _GIB),
            "fatal",
            f"pipeline raw bytes <= {config.new_raw_soft_cap_gib:g} GiB",
            raw_bytes,
            "catalogued pipeline-acquired raw bytes",
        ),
        _check(
            "storage.reserve",
            disk_free >= int(config.minimum_free_gib * _GIB),
            "fatal",
            f"disk reserve >= {config.minimum_free_gib:g} GiB",
            disk_free,
            "available filesystem reserve",
        ),
    ]




def checks(paths: ProjectPaths, config: StudyAreaConfig) -> list[CheckResult]:
    return _provenance_checks(paths, config)
