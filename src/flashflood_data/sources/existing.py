"""Rules and adapter for registering legacy files without moving them."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Final

from flashflood_data.inventory import (
    InventoryConflict,
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

_REUSABLE_STATUSES = frozenset({AssetStatus.VALIDATED, AssetStatus.HARMONIZED, AssetStatus.DERIVED})
_RECOVERABLE_STATUSES = frozenset(
    {AssetStatus.DISCOVERED, AssetStatus.FETCHING, AssetStatus.FETCHED}
)


@dataclass(frozen=True)
class _Candidate:
    relative: str
    rule: InventoryRule
    primary: Path
    members: tuple[Path, ...]
    member_relatives: tuple[str, ...]
    checksum: str
    size_bytes: int


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


def _resolve_confined(dataset: Path, candidate: Path) -> Path:
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise InventoryConflict(f"unreadable inventory candidate: {candidate.name}") from exc
    if not resolved.is_relative_to(dataset):
        raise InventoryConflict(f"inventory candidate escapes dataset: {candidate.name}")
    return resolved


def _same_immutable_asset(existing: AssetRecord, candidate: AssetRecord) -> bool:
    fields = (
        "asset_id",
        "source_id",
        "source_version",
        "kind",
        "source_uri",
        "storage_path",
        "media_type",
        "license_id",
        "size_bytes",
        "checksum_algorithm",
        "checksum",
        "source_valid_time",
    )
    return all(getattr(existing, field) == getattr(candidate, field) for field in fields)


def _finish_validation(
    context: SourceContext,
    record: AssetRecord,
    validation: ValidationResult,
) -> AssetRecord:
    current = record
    if validation.passed:
        if current.status is AssetStatus.FETCHING:
            current = context.catalog.transition(current.asset_id, AssetStatus.FETCHED)
        return context.catalog.transition(current.asset_id, AssetStatus.VALIDATED)
    return context.catalog.transition(
        current.asset_id,
        AssetStatus.FAILED,
        error_code="known_format_validation_failed",
        error_message="legacy asset failed a readability or format check",
    )


def inventory_existing(
    context: SourceContext,
    *,
    rules: Sequence[InventoryRule] = DEFAULT_RULES,
    rehash: bool = False,
) -> list[AssetRecord]:
    """Register every matching legacy asset in place and preserve duplicates."""
    dataset = context.paths.dataset
    resolved_dataset = dataset.resolve(strict=True)
    cache_path = context.paths.catalog / "inventory-cache.json"
    old_cache = read_checksum_cache(cache_path)
    new_cache: dict[str, dict[str, int | str]] = {}
    candidates: list[_Candidate] = []

    for rule in rules:
        for primary in dataset.glob(rule.glob):
            if _excluded(dataset, primary):
                continue
            resolved_primary = _resolve_confined(resolved_dataset, primary)
            if not resolved_primary.is_file():
                continue
            relative = primary.relative_to(dataset).as_posix()
            member_pairs: list[tuple[str, Path, Path]] = []
            for suffix in rule.bundle_suffixes:
                member = bundle_member(primary, suffix)
                if _excluded(dataset, member):
                    continue
                if not member.exists() and not member.is_symlink():
                    continue
                resolved_member = _resolve_confined(resolved_dataset, member)
                if resolved_member.is_file():
                    member_pairs.append((suffix, member, resolved_member))
            checksums = [
                (
                    suffix,
                    cached_sha256(
                        resolved_member,
                        dataset_root=dataset,
                        cache_path=member,
                        old_entries=old_cache,
                        new_entries=new_cache,
                        rehash=rehash,
                    ),
                )
                for suffix, member, resolved_member in member_pairs
            ]
            checksum = compound_checksum(checksums)
            candidates.append(
                _Candidate(
                    relative=relative,
                    rule=rule,
                    primary=resolved_primary,
                    members=tuple(item[2] for item in member_pairs),
                    member_relatives=tuple(
                        item[1].relative_to(dataset).as_posix() for item in member_pairs
                    ),
                    checksum=checksum,
                    size_bytes=sum(item[2].stat().st_size for item in member_pairs),
                )
            )

    candidates.sort(key=lambda candidate: candidate.relative)
    canonical_by_content: dict[tuple[str, str, AssetKind, str, str], str] = {}
    records: list[AssetRecord] = []
    for candidate in candidates:
        rule = candidate.rule
        identity = (
            rule.source_id,
            rule.version,
            rule.kind,
            rule.media_type,
            candidate.checksum,
        )
        duplicate_of = canonical_by_content.get(identity)
        asset_id = _asset_id(rule, candidate.relative)
        canonical_by_content.setdefault(identity, asset_id)
        metadata = {
            "bundle_members": list(candidate.member_relatives),
            "inventory_glob": rule.glob,
        }
        discovered = AssetRecord(
            asset_id=asset_id,
            source_id=rule.source_id,
            source_version=rule.version,
            kind=rule.kind,
            source_uri=candidate.primary.as_uri(),
            storage_path=str(candidate.primary),
            media_type=rule.media_type,
            size_bytes=candidate.size_bytes,
            checksum=candidate.checksum,
            retrieved_at=file_timestamp(candidate.primary),
            license_id=_LICENSES.get(rule.source_id, "unclassified-legacy-source"),
            pipeline_run_id=context.run_id,
            status=AssetStatus.DISCOVERED,
            duplicate_of_asset_id=duplicate_of,
            metadata_json=json.dumps(metadata, sort_keys=True, separators=(",", ":")),
        )
        try:
            existing = context.catalog.get(asset_id)
        except KeyError:
            existing = None
        if existing is not None:
            if not _same_immutable_asset(existing, discovered):
                raise InventoryConflict(f"conflicting existing inventory asset: {asset_id}")
            if existing.status is AssetStatus.QUARANTINED:
                raise InventoryConflict(f"quarantined existing inventory asset: {asset_id}")
            if existing.status in _REUSABLE_STATUSES:
                records.append(existing)
                continue
            if existing.status not in _RECOVERABLE_STATUSES:
                raise InventoryConflict(f"non-recoverable existing inventory asset: {asset_id}")

        validation = validate_known_format(candidate.primary, rule.media_type, candidate.members)
        current = existing
        if current is None:
            current = context.catalog.upsert(discovered)
        record = _finish_validation(context, current, validation)
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
