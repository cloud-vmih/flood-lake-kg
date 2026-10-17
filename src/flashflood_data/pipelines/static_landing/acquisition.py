"""Source acquisition and exact raw-object selection without harmonization."""

import json
from collections.abc import Callable, Sequence
from hashlib import sha256
from pathlib import Path

import geopandas as gpd

from flashflood_data.catalog.models import AssetRecord, AssetStatus, RemoteAsset, SourceSpec
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.pipelines.static_landing.adapters.base import SourceAdapter, SourceContext
from flashflood_data.pipelines.static_landing.adapters.hydro_subset import build_hydro_subset
from flashflood_data.pipelines.static_landing.adapters.registry import build_adapter
from flashflood_data.pipelines.static_landing.bundle import (
    build_deterministic_zip,
    shapefile_members,
)
from flashflood_data.pipelines.static_landing.config import LandingSourcePolicy
from flashflood_data.pipelines.static_landing.models import PreparedObject
from flashflood_data.shared.quality import DataQualityBlock, QualityEvaluation, QualityFinding
from flashflood_data.storage.http import PayloadMismatch
from flashflood_data.storage.http.fetcher import HttpFetcher


class SourceLandingError(RuntimeError):
    """Base class for sanitized per-source landing failures."""

    code = "source_landing_failed"


class MissingSourceAssets(SourceLandingError):
    code = "missing_source_assets"


class AmbiguousSourceSelection(SourceLandingError):
    code = "ambiguous_source_selection"


class RawSourceValidationError(SourceLandingError):
    code = "source_validation_failed"


class SourcePreconditionError(SourceLandingError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _quality_block(
    asset_id: str,
    rule_id: str,
    *,
    observed: object,
    expected: object | None,
    evidence_uri: str | None = None,
) -> DataQualityBlock:
    evaluation = QualityEvaluation.from_parts(
        accepted_rows=(),
        rejected_rows=(),
        findings=(
            QualityFinding(
                rule_id=rule_id,
                rule_version="v1",
                status="failed",
                severity="fatal",
                scope_key=asset_id,
                observed_value=observed,
                expected_value=expected,
                failed_row_count=1,
            ),
        ),
        input_row_count=0,
    )
    return DataQualityBlock(
        f"raw source payload failed {rule_id}",
        evaluation,
        evidence_uri=evidence_uri,
    )


def _quarantine_evidence_uri(context: SourceContext, asset_id: str) -> str | None:
    try:
        record = context.catalog.get(asset_id)
    except KeyError:
        return None
    path = Path(record.storage_path)
    return path.resolve().as_uri() if path.is_file() else None


def ensure_environmental_aoi(context: SourceContext) -> None:
    """Validate the AOI required before DEM and soil remote resolution."""
    path = context.paths.harmonized / "aoi" / "environmental_aoi.geoparquet"
    if not path.is_file():
        raise SourcePreconditionError("environmental_aoi_missing")
    try:
        layer = gpd.read_parquet(path)
        if layer.empty or layer.crs is None:
            raise ValueError
        geographic = layer.to_crs("EPSG:4326")
        if geographic.geometry.is_empty.any() or geographic.geometry.union_all().is_empty:
            raise ValueError
    except (OSError, ValueError):
        raise SourcePreconditionError("environmental_aoi_invalid") from None


def _national_hydrological_aoi(paths: ProjectPaths) -> Path:
    path = paths.harmonized / "aoi" / "vietnam_hydrological_aoi.geoparquet"
    if not path.is_file():
        raise SourcePreconditionError("vietnam_hydrological_aoi_missing")
    try:
        layer = gpd.read_parquet(path)
        if (
            layer.empty
            or layer.crs is None
            or layer.geometry.is_empty.any()
            or layer.geometry.union_all().is_empty
        ):
            raise ValueError
    except (OSError, ValueError):
        raise SourcePreconditionError("vietnam_hydrological_aoi_invalid") from None
    return path


def _reusable(
    remote: RemoteAsset,
    available: Sequence[AssetRecord],
    is_verified: Callable[[AssetRecord], bool],
) -> bool:
    for record in available:
        if (
            record.asset_id == remote.asset_id
            and record.source_id == remote.source_id
            and record.source_version == remote.source_version
            and record.status is AssetStatus.VALIDATED
            and record.duplicate_of_asset_id is None
            and is_verified(record)
            and (remote.expected_checksum is None or remote.expected_checksum == record.checksum)
        ):
            return True
    return False


def _fetch_with_adapter_hook(
    adapter: SourceAdapter,
    fetcher: HttpFetcher,
    context: SourceContext,
    remote: RemoteAsset,
) -> AssetRecord:
    hook = getattr(adapter, "fetch_raw", None)
    if callable(hook):
        return hook(fetcher, context, remote)
    return fetcher.fetch(remote, context.run_id)


def acquire_validated_assets(
    policy: LandingSourcePolicy,
    base_spec: SourceSpec,
    context: SourceContext,
    fetcher: HttpFetcher,
) -> tuple[AssetRecord, ...]:
    """Acquire or reuse source payloads and stop after raw validation."""
    if policy.source_id in {
        "cop_dem_glo30_2024_1",
        "soilgrids_2_0",
        "esa_worldcover_2021_v200",
    }:
        ensure_environmental_aoi(context)
    spec = base_spec.model_copy(
        update={"settings": dict(base_spec.settings) | dict(policy.settings_override)}
    )
    adapter = build_adapter(spec)
    verification_cache: dict[tuple[str, str, str, int], bool] = {}

    def is_verified(record: AssetRecord) -> bool:
        identity = (
            record.asset_id,
            record.checksum,
            record.storage_path,
            record.size_bytes,
        )
        if identity not in verification_cache:
            verification_cache[identity] = context.catalog.has_verified_content(record)
        return verification_cache[identity]

    while True:
        available = context.catalog.raw_assets(spec.source_id)
        verified_available = [
            record
            for record in available
            if record.status is AssetStatus.VALIDATED
            and record.duplicate_of_asset_id is None
            and is_verified(record)
        ]
        remotes = adapter.resolve(context, verified_available)
        pending = [remote for remote in remotes if not _reusable(remote, available, is_verified)]
        if not pending:
            break
        for remote in pending:
            target = (context.paths.dataset / remote.target_relative_path).resolve()
            adopted = None
            adopt_local = getattr(fetcher, "adopt_local", None)
            if (
                callable(adopt_local)
                and target.is_relative_to(context.paths.raw.resolve())
                and target.is_file()
            ):
                adopted = adopt_local(
                    remote,
                    context.run_id,
                    adapter.validate_raw,
                )
            try:
                fetched = adopted or _fetch_with_adapter_hook(adapter, fetcher, context, remote)
            except PayloadMismatch as error:
                error_code = str(error).partition(":")[0]
                rule_id = "object_checksum" if "checksum" in error_code else "object_size"
                raise _quality_block(
                    remote.asset_id,
                    rule_id,
                    observed={"validation_error": error_code},
                    expected={"matches_provider_manifest": True},
                    evidence_uri=_quarantine_evidence_uri(context, remote.asset_id),
                ) from error
            if fetched.status is AssetStatus.VALIDATED:
                continue
            validation = adapter.validate_raw(Path(fetched.storage_path))
            if not validation.passed:
                context.catalog.transition(
                    fetched.asset_id,
                    AssetStatus.FAILED,
                    error_code="raw_validation_failed",
                    error_message="raw payload validation failed",
                )
                evidence = Path(fetched.storage_path)
                raise _quality_block(
                    fetched.asset_id,
                    "object_readable",
                    observed={
                        "checks": dict(validation.checks),
                        "metrics": dict(validation.metrics),
                    },
                    expected={"adapter_validation_passed": True},
                    evidence_uri=evidence.resolve().as_uri() if evidence.is_file() else None,
                )
            context.catalog.transition(fetched.asset_id, AssetStatus.VALIDATED)
    selected_ids = None
    if spec.source_id == "soilgrids_2_0":
        selected_ids = {remote.asset_id for remote in remotes}
        selected_ids.update(
            record.asset_id for record in available if record.asset_id.endswith("-capabilities")
        )
    return tuple(
        record
        for record in context.catalog.raw_assets(spec.source_id)
        if record.status is AssetStatus.VALIDATED
        and record.duplicate_of_asset_id is None
        and is_verified(record)
        and (selected_ids is None or record.asset_id in selected_ids)
    )


def _metadata_member_paths(record: AssetRecord) -> tuple[Path, ...]:
    metadata = json.loads(record.metadata_json)
    raw_members = metadata.get("bundle_members")
    if not isinstance(raw_members, list) or not all(isinstance(item, str) for item in raw_members):
        raise MissingSourceAssets(f"{record.asset_id} has no audited bundle members")
    primary = Path(record.storage_path).resolve()
    paths = tuple(Path(item) for item in raw_members)
    if all(path.is_absolute() for path in paths):
        return tuple(path.resolve() for path in paths)
    primary_relative = next((path for path in paths if path.name == primary.name), None)
    if primary_relative is None:
        raise MissingSourceAssets(f"{record.asset_id} bundle omits its primary member")
    root = primary.parents[len(primary_relative.parts) - 1]
    resolved: list[Path] = []
    for path in paths:
        candidate = (root / path).resolve()
        if not candidate.is_relative_to(root):
            raise MissingSourceAssets(f"{record.asset_id} bundle member escapes dataset")
        resolved.append(candidate)
    return tuple(resolved)


def _prepared(record: AssetRecord, *, selection: dict[str, object]) -> PreparedObject:
    path = Path(record.storage_path)
    return PreparedObject(
        source_id=record.source_id,
        source_version=record.source_version,
        asset_id=record.asset_id,
        path=path,
        filename=path.name,
        media_type=record.media_type,
        source_uri=record.source_uri,
        license_id=record.license_id,
        retrieved_at=record.retrieved_at,
        source_valid_time=record.source_valid_time,
        selection=selection,
        provider_metadata=json.loads(record.metadata_json),
    )


def _prepare_shapefile_bundle(
    policy: LandingSourcePolicy,
    records: Sequence[AssetRecord],
    staging_root: Path,
    run_id: str,
) -> tuple[PreparedObject, ...]:
    selected = [
        record
        for record in records
        if record.source_id == policy.source_id
        and record.status is AssetStatus.VALIDATED
        and record.duplicate_of_asset_id is None
        and policy.filename_contains in Path(record.storage_path).name
    ]
    if not selected:
        raise MissingSourceAssets(f"no canonical validated source for {policy.source_id}")
    if len(selected) != 1:
        raise AmbiguousSourceSelection(policy.source_id)
    record = selected[0]
    members = shapefile_members(Path(record.storage_path), _metadata_member_paths(record))
    output = staging_root / run_id / policy.source_id / str(policy.output_name)
    bundle = build_deterministic_zip(members, output)
    return (
        PreparedObject(
            source_id=record.source_id,
            source_version=record.source_version,
            asset_id=record.asset_id,
            path=bundle.path,
            filename=bundle.path.name,
            media_type="application/zip",
            source_uri=record.source_uri,
            license_id=record.license_id,
            retrieved_at=record.retrieved_at,
            source_valid_time=record.source_valid_time,
            selection=dict(policy.selection),
            members=bundle.members,
            source_archive_checksum=record.checksum,
            source_archive_size_bytes=record.size_bytes,
            provider_metadata=json.loads(record.metadata_json),
        ),
    )


def _selected_record(policy: LandingSourcePolicy, records: Sequence[AssetRecord]) -> AssetRecord:
    selected = [
        record
        for record in records
        if record.source_id == policy.source_id
        and record.status is AssetStatus.VALIDATED
        and record.duplicate_of_asset_id is None
        and policy.filename_contains in Path(record.storage_path).name
    ]
    if not selected:
        raise MissingSourceAssets(f"no canonical validated source for {policy.source_id}")
    if len(selected) != 1:
        raise AmbiguousSourceSelection(policy.source_id)
    return selected[0]


def _ids_checksum(ids: Sequence[int]) -> str:
    payload = ",".join(map(str, ids)).encode("ascii")
    return sha256(payload).hexdigest()


def _prepare_hydro_subset(
    policy: LandingSourcePolicy,
    records: Sequence[AssetRecord],
    staging_root: Path,
    run_id: str,
    paths: ProjectPaths,
) -> tuple[PreparedObject, ...]:
    record = _selected_record(policy, records)
    aoi_path = _national_hydrological_aoi(paths)
    output_dir = staging_root / run_id / policy.source_id / "subset"
    selected_ids = None
    if policy.source_id == "basinatlas_v10":
        hydro_path = paths.dataset / "hybas_as_lev01-12_v1c" / "hybas_as_lev12_v1c.shp"
        hydro_result = build_hydro_subset(
            "hydrobasins_v1c", hydro_path, aoi_path, output_dir / "hydrobasins"
        )
        selected_ids = hydro_result.selected_hybas_ids
    result = build_hydro_subset(
        policy.source_id,
        Path(record.storage_path),
        aoi_path,
        output_dir,
        selected_hybas_ids=selected_ids,
    )
    candidates = tuple(result.path.parent.glob(f"{result.path.stem}.*"))
    members = shapefile_members(result.path, candidates)
    output = staging_root / run_id / policy.source_id / str(policy.output_name)
    bundle = build_deterministic_zip(members, output)
    hybas_checksum = _ids_checksum(result.selected_hybas_ids)
    selection = {
        **dict(policy.selection),
        "upstream_hops": 1,
        "spatial_scope_id": f"vietnam-l12-h1-{result.aoi_checksum[:12]}",
        "aoi_checksum": result.aoi_checksum,
        "selection_version": result.selection_version,
        "source_feature_count": result.source_feature_count,
        "selected_feature_count": result.selected_feature_count,
        "selected_hybas_ids_checksum": hybas_checksum,
    }
    return (
        PreparedObject(
            source_id=record.source_id,
            source_version=record.source_version,
            asset_id=record.asset_id,
            path=bundle.path,
            filename=bundle.path.name,
            media_type="application/zip",
            source_uri=record.source_uri,
            license_id=record.license_id,
            retrieved_at=record.retrieved_at,
            source_valid_time=record.source_valid_time,
            selection=selection,
            members=bundle.members,
            source_archive_checksum=record.checksum,
            source_archive_size_bytes=record.size_bytes,
            provider_metadata={
                **json.loads(record.metadata_json),
                "curated_subset_checksum": bundle.checksum,
            },
        ),
    )


def _prepare_soilgrids(
    policy: LandingSourcePolicy, records: Sequence[AssetRecord]
) -> tuple[PreparedObject, ...]:
    properties = tuple(str(item) for item in policy.settings_override.get("properties", ()))
    depths = tuple(str(item) for item in policy.settings_override.get("depths", ()))
    statistics = tuple(str(item) for item in policy.settings_override.get("statistics", ()))
    expected = {
        f"soilgrids-2-0-{property_id}-capabilities": {
            "property": property_id,
            "kind": "capabilities",
        }
        for property_id in properties
    }
    for property_id in properties:
        for depth in depths:
            for statistic in statistics:
                expected[f"soilgrids-2-0-{property_id}-{depth}-{statistic}"] = {
                    "property": property_id,
                    "depth": depth,
                    "statistic": statistic,
                }
    indexed = {
        record.asset_id: record
        for record in records
        if record.source_id == policy.source_id
        and record.status is AssetStatus.VALIDATED
        and record.duplicate_of_asset_id is None
    }
    selected: dict[str, AssetRecord] = {}
    for expected_id in expected:
        matches = [
            record
            for asset_id, record in indexed.items()
            if asset_id == expected_id or asset_id.startswith(f"{expected_id}--")
        ]
        if len(matches) > 1:
            raise AmbiguousSourceSelection(expected_id)
        if matches:
            selected[expected_id] = matches[0]
    missing_ids = sorted(set(expected) - set(selected))
    if missing_ids:
        missing_paths = []
        for asset_id in missing_ids:
            selection = expected[asset_id]
            property_id = selection["property"]
            if selection.get("kind") == "capabilities":
                missing_paths.append(f"{property_id}/capabilities.xml")
            else:
                missing_paths.append(
                    f"{property_id}/{selection['depth']}/{selection['statistic']}.tif"
                )
        raise MissingSourceAssets(", ".join(missing_paths))
    return tuple(
        _prepared(selected[asset_id], selection=expected[asset_id]) for asset_id in sorted(expected)
    )


def prepare_source_objects(
    policy: LandingSourcePolicy,
    records: Sequence[AssetRecord],
    *,
    staging_root: Path,
    run_id: str,
    paths: ProjectPaths | None = None,
) -> tuple[PreparedObject, ...]:
    """Select exact canonical objects and package multi-file source formats."""
    if policy.mode == "shapefile_bundle":
        return _prepare_shapefile_bundle(policy, records, staging_root, run_id)
    if policy.mode == "hydro_subset_bundle":
        if paths is None:
            raise ValueError("hydro subset preparation requires project paths")
        return _prepare_hydro_subset(policy, records, staging_root, run_id, paths)
    if policy.source_id == "soilgrids_2_0":
        return _prepare_soilgrids(policy, records)
    selected = [
        record
        for record in records
        if record.source_id == policy.source_id
        and record.status is AssetStatus.VALIDATED
        and record.duplicate_of_asset_id is None
    ]
    if not selected:
        raise MissingSourceAssets(policy.source_id)
    return tuple(
        _prepared(
            record,
            selection={
                **dict(policy.selection),
                **(
                    {"tile_id": Path(record.storage_path).parent.name}
                    if policy.source_id == "cop_dem_glo30_2024_1"
                    else {}
                ),
            },
        )
        for record in sorted(selected, key=lambda item: item.asset_id)
    )
