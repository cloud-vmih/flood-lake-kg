"""Source acquisition and exact raw-object selection without harmonization."""

import json
from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd

from flashflood_data.catalog import AssetCatalog
from flashflood_data.catalog.models import AssetRecord, AssetStatus, RemoteAsset, SourceSpec
from flashflood_data.orchestration.landing.bundle import (
    build_deterministic_zip,
    shapefile_members,
)
from flashflood_data.orchestration.landing.config import LandingSourcePolicy
from flashflood_data.orchestration.landing.models import PreparedObject
from flashflood_data.static.sources.base import SourceAdapter, SourceContext
from flashflood_data.static.sources.registry import build_adapter
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


def _reusable(remote: RemoteAsset, available: Sequence[AssetRecord], catalog: AssetCatalog) -> bool:
    for record in available:
        if (
            record.asset_id == remote.asset_id
            and record.source_id == remote.source_id
            and record.source_version == remote.source_version
            and record.status is AssetStatus.VALIDATED
            and record.duplicate_of_asset_id is None
            and catalog.has_verified_content(record)
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
    if policy.source_id in {"cop_dem_glo30_2024_1", "soilgrids_2_0"}:
        ensure_environmental_aoi(context)
    spec = base_spec.model_copy(
        update={"settings": dict(base_spec.settings) | dict(policy.settings_override)}
    )
    adapter = build_adapter(spec)
    while True:
        available = context.catalog.raw_assets(spec.source_id)
        remotes = adapter.resolve(context, available)
        pending = [
            remote
            for remote in remotes
            if not _reusable(remote, available, context.catalog)
        ]
        if not pending:
            break
        for remote in pending:
            fetched = _fetch_with_adapter_hook(adapter, fetcher, context, remote)
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
                raise RawSourceValidationError(fetched.asset_id)
            context.catalog.transition(fetched.asset_id, AssetStatus.VALIDATED)
    return tuple(
        record
        for record in context.catalog.raw_assets(spec.source_id)
        if record.status is AssetStatus.VALIDATED
        and record.duplicate_of_asset_id is None
        and context.catalog.has_verified_content(record)
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


def _prepare_soilgrids(
    policy: LandingSourcePolicy, records: Sequence[AssetRecord]
) -> tuple[PreparedObject, ...]:
    properties = tuple(str(item) for item in policy.settings_override.get("properties", ()))
    depths = tuple(str(item) for item in policy.settings_override.get("depths", ()))
    statistics = tuple(str(item) for item in policy.settings_override.get("statistics", ()))
    expected = {
        f"soilgrids-2-0-{property_id}-capabilities": {"property": property_id, "kind": "capabilities"}
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
    missing_ids = sorted(set(expected) - set(indexed))
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
        _prepared(indexed[asset_id], selection=expected[asset_id]) for asset_id in sorted(expected)
    )


def prepare_source_objects(
    policy: LandingSourcePolicy,
    records: Sequence[AssetRecord],
    *,
    staging_root: Path,
    run_id: str,
) -> tuple[PreparedObject, ...]:
    """Select exact canonical objects and package multi-file source formats."""
    if policy.mode == "shapefile_bundle":
        return _prepare_shapefile_bundle(policy, records, staging_root, run_id)
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
