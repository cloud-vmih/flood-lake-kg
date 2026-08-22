"""Rules and adapter for registering legacy files without moving them."""

from __future__ import annotations

import json
from collections.abc import Sequence
from hashlib import sha256
from pathlib import Path
from typing import Final

from flashflood_data.inventory import (
    InventoryRule,
    bundle_member,
    cached_sha256,
    compound_checksum,
    deterministic_report,
    file_timestamp,
    read_checksum_cache,
    validate_known_format,
    write_json_atomic,
)
from flashflood_data.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    ValidationResult,
)
from flashflood_data.sources.base import SourceAdapter, SourceContext

_SHAPEFILE_SUFFIXES = (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx", ".shp.xml")

DEFAULT_RULES: Final[tuple[InventoryRule, ...]] = (
    InventoryRule(
        "hydrobasins_v1c",
        "1c",
        AssetKind.RAW,
        "hybas_as_lev01-12_v1c/hybas_as_lev??_v1c.shp",
        "application/x-esri-shapefile",
        _SHAPEFILE_SUFFIXES,
    ),
    InventoryRule(
        "hydrobasins_lake_sample_v1c",
        "1c",
        AssetKind.RAW,
        "Data/**/hybas_lake_as_lev08_v1c.shp",
        "application/x-esri-shapefile",
        _SHAPEFILE_SUFFIXES,
    ),
    InventoryRule(
        "basinatlas_v10",
        "10",
        AssetKind.RAW,
        "BasinATLAS_Data_v10_shp/BasinATLAS_v10_shp/BasinATLAS_v10_lev??.shp",
        "application/x-esri-shapefile",
        (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx"),
    ),
    InventoryRule(
        "basinatlas_archive_v10",
        "10",
        AssetKind.RAW,
        "BasinATLAS_Data_v10_shp.zip",
        "application/zip",
        (".zip",),
    ),
    InventoryRule(
        "hydrorivers_v10",
        "10",
        AssetKind.RAW,
        "HydroRIVERS_v10_as_shp/**/HydroRIVERS_v10_as.shp",
        "application/x-esri-shapefile",
        (".shp", ".shx", ".dbf", ".prj", ".sbn", ".sbx"),
    ),
    InventoryRule(
        "worldpop_vnm_2025",
        "R2025A-v1",
        AssetKind.RAW,
        "Data/**/vnm_pop_2025_CN_100m_R2025A_v1.tif",
        "image/tiff",
        (".tif",),
    ),
    InventoryRule(
        "historical_flood_evidence_2020_2026",
        "2026-08-14",
        AssetKind.RAW,
        "Lu_Son_La_2020_2026.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        (".xlsx",),
    ),
    InventoryRule(
        "legacy_population_sample",
        "unversioned",
        AssetKind.HARMONIZED,
        "Data/hybas_vnm_with_pop.shp",
        "application/x-esri-shapefile",
        (".shp", ".shx", ".dbf", ".prj", ".cpg"),
    ),
    InventoryRule(
        "legacy_population_sample",
        "unversioned",
        AssetKind.DERIVED,
        "Data/pop_by_basin_vnm.csv",
        "text/csv",
        (".csv",),
    ),
    InventoryRule(
        "legacy_population_sample_script",
        "unversioned",
        AssetKind.RAW,
        "Data/main.py",
        "text/x-python",
        (".py",),
    ),
)

_LICENSES: Final[dict[str, str]] = {
    "hydrobasins_v1c": "HydroSHEDS-free-academic",
    "hydrobasins_lake_sample_v1c": "HydroSHEDS-free-academic",
    "basinatlas_v10": "HydroATLAS-free-academic",
    "basinatlas_archive_v10": "HydroATLAS-free-academic",
    "hydrorivers_v10": "HydroSHEDS-free-academic",
    "worldpop_vnm_2025": "CC-BY-4.0",
    "historical_flood_evidence_2020_2026": "project-evidence-compilation",
    "legacy_population_sample": "project-legacy-unversioned",
    "legacy_population_sample_script": "project-legacy-unversioned",
}


def _excluded(dataset: Path, path: Path) -> bool:
    relative = path.relative_to(dataset)
    parts = relative.parts
    if not parts:
        return True
    if parts[0] in {"raw", "harmonized", "derived", "catalog", "qa"}:
        return True
    if len(parts) >= 2 and parts[:2] == ("Data", "venv"):
        return True
    return any(part.endswith(".partial") for part in parts)


def _asset_id(rule: InventoryRule, relative: str) -> str:
    identity = f"{rule.source_id}\0{rule.version}\0{rule.kind.value}\0{relative}"
    return f"existing-{sha256(identity.encode('utf-8')).hexdigest()[:24]}"


def inventory_existing(
    context: SourceContext,
    *,
    rules: Sequence[InventoryRule] = DEFAULT_RULES,
    rehash: bool = False,
) -> list[AssetRecord]:
    """Register every matching legacy asset in place and preserve duplicates."""
    dataset = context.paths.dataset
    cache_path = context.paths.catalog / "inventory-cache.json"
    old_cache = read_checksum_cache(cache_path)
    new_cache: dict[str, dict[str, int | str]] = {}
    candidates: list[tuple[str, InventoryRule, Path, tuple[Path, ...], str, int]] = []

    for rule in rules:
        for primary in dataset.glob(rule.glob):
            if not primary.is_file() or _excluded(dataset, primary):
                continue
            relative = primary.relative_to(dataset).as_posix()
            members = tuple(
                member
                for suffix in rule.bundle_suffixes
                if (member := bundle_member(primary, suffix)).is_file()
                and not _excluded(dataset, member)
            )
            checksums = [
                (
                    suffix,
                    cached_sha256(
                        member,
                        dataset_root=dataset,
                        old_entries=old_cache,
                        new_entries=new_cache,
                        rehash=rehash,
                    ),
                )
                for suffix in rule.bundle_suffixes
                if (member := bundle_member(primary, suffix)) in members
            ]
            checksum = compound_checksum(checksums)
            candidates.append(
                (
                    relative,
                    rule,
                    primary,
                    members,
                    checksum,
                    sum(member.stat().st_size for member in members),
                )
            )

    candidates.sort(key=lambda candidate: candidate[0])
    canonical_by_content: dict[tuple[str, str, AssetKind, str, str], str] = {}
    records: list[AssetRecord] = []
    for relative, rule, primary, members, checksum, size_bytes in candidates:
        identity = (rule.source_id, rule.version, rule.kind, rule.media_type, checksum)
        duplicate_of = canonical_by_content.get(identity)
        asset_id = _asset_id(rule, relative)
        canonical_by_content.setdefault(identity, asset_id)
        metadata = {
            "bundle_members": [member.relative_to(dataset).as_posix() for member in members],
            "inventory_glob": rule.glob,
        }
        discovered = AssetRecord(
            asset_id=asset_id,
            source_id=rule.source_id,
            source_version=rule.version,
            kind=rule.kind,
            source_uri=primary.resolve().as_uri(),
            storage_path=str(primary.resolve()),
            media_type=rule.media_type,
            size_bytes=size_bytes,
            checksum=checksum,
            retrieved_at=file_timestamp(primary),
            license_id=_LICENSES.get(rule.source_id, "unclassified-legacy-source"),
            pipeline_run_id=context.run_id,
            status=AssetStatus.DISCOVERED,
            duplicate_of_asset_id=duplicate_of,
            metadata_json=json.dumps(metadata, sort_keys=True, separators=(",", ":")),
        )
        context.catalog.upsert(discovered)
        validation = validate_known_format(primary, rule.media_type, members)
        if validation.passed:
            record = context.catalog.transition(asset_id, AssetStatus.VALIDATED)
        else:
            record = context.catalog.transition(
                asset_id,
                AssetStatus.FAILED,
                error_code="known_format_validation_failed",
                error_message="legacy asset failed a readability or format check",
            )
        records.append(record)

    write_json_atomic(cache_path, {"entries": dict(sorted(new_cache.items()))})
    write_json_atomic(context.paths.catalog / "inventory.json", deterministic_report(records))
    return records


class ExistingAdapter(SourceAdapter):
    """Adapter boundary for sources that already exist in the local data lake."""

    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        return []

    def validate_raw(self, path: Path) -> ValidationResult:
        media_type = {
            ".shp": "application/x-esri-shapefile",
            ".tif": "image/tiff",
            ".zip": "application/zip",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".csv": "text/csv",
            ".py": "text/x-python",
        }.get(path.suffix.lower(), "application/octet-stream")
        return validate_known_format(path, media_type, (path,))

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        return list(assets)
