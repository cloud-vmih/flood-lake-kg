"""Manifest-driven, resumable orchestration for static source adapters."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.catalog.inventory import compound_checksum
from flashflood_data.catalog.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    RunRecord,
    SourceSpec,
)
from flashflood_data.core.config import EnvironmentSettings, StudyAreaConfig, load_study_area
from flashflood_data.core.paths import ProjectPaths
from flashflood_data.static.sources.base import (
    SourceAdapter,
    SourceConfigurationError,
    SourceContext,
)
from flashflood_data.static.sources.budget import StorageBudget
from flashflood_data.static.sources.cop_dem import MissingCredentials
from flashflood_data.static.sources.existing import inventory_existing
from flashflood_data.static.sources.registry import (
    UnsupportedAdapter,
    build_adapter,
    load_source_specs,
)
from flashflood_data.static.workflow.fingerprint import dependency_fingerprint
from flashflood_data.static.workflow.stages import Stage
from flashflood_data.static.workflow.summary import RunSummary
from flashflood_data.storage.http import BudgetRejected, HttpFetcher

_HYDRO_SOURCES = frozenset({"hydrobasins_v1c", "basinatlas_v10", "hydrorivers_v10"})
_HYDRO_COMPOSITE_OWNER = "hydrobasins_v1c"
_BOOTSTRAP_SOURCES = ("sonla_admin_2025", "gadm_vnm_4_1")
_PROCESSOR_VERSION = "0.1.0"
_CONFIGURATION_ERRORS = (
    BudgetRejected,
    MissingCredentials,
    SourceConfigurationError,
    UnsupportedAdapter,
)

StageHandler = Callable[["StaticPipeline", Stage, str, SourceContext], list[AssetRecord]]


class StaticPipeline:
    """Run fixed stages while leaving adapter-specific data logic at the boundary."""

    def __init__(
        self,
        paths: ProjectPaths | None = None,
        *,
        profile: str = "smoke",
        source_specs: Mapping[str, SourceSpec] | None = None,
        adapter_factory: Callable[[SourceSpec], SourceAdapter] = build_adapter,
        fetcher: object | None = None,
        stage_handlers: Mapping[Stage, StageHandler] | None = None,
    ) -> None:
        self.paths = paths or ProjectPaths.discover()
        self.paths.ensure_output_dirs()
        self.profile = profile
        self.catalog = AssetCatalog(self.paths)
        study_path = self.paths.root / "config" / "study_area.yaml"
        self.study_area = load_study_area(study_path) if study_path.is_file() else StudyAreaConfig()
        self.environment = EnvironmentSettings(_env_file=self.paths.root / ".env")
        self.source_specs = dict(
            source_specs
            if source_specs is not None
            else load_source_specs(self.paths.root / "config" / "sources")
        )
        self.adapter_factory = adapter_factory
        self.fetcher = fetcher or HttpFetcher(
            self.paths,
            self.catalog,
            StorageBudget.from_config(self.paths.dataset, self.study_area),
            environment=self.environment,
        )
        if stage_handlers is None:
            from flashflood_data.static.workflow.handlers import default_stage_handlers

            self._stage_handlers = dict(default_stage_handlers())
        else:
            self._stage_handlers = dict(stage_handlers)
        self._latest_hydro_metrics: dict[str, int] = {}

    def register_stage_handler(self, stage: Stage, handler: StageHandler) -> None:
        """Register a later-stage implementation without changing CLI dispatch."""
        self._stage_handlers[stage] = handler

    def run(
        self,
        stages: Sequence[Stage],
        source_ids: Sequence[str] | None = None,
        *,
        resolve_only: bool = False,
        command: str = "run-static",
    ) -> RunSummary:
        """Execute selected stages, retaining successes when sibling sources fail."""
        normalized_stages = tuple(Stage(stage) for stage in stages)
        if not normalized_stages:
            raise ValueError("at least one stage is required")
        selected = self._selected_sources(source_ids)
        config_fingerprint = dependency_fingerprint(
            (), self.study_area.model_dump(mode="json"), _PROCESSOR_VERSION
        )
        run_id = f"{command}-{uuid4().hex}"
        self.catalog.begin_run(
            RunRecord(
                run_id=run_id,
                command=command,
                started_at=datetime.now(UTC),
                config_fingerprint=config_fingerprint,
            )
        )
        summary = RunSummary(run_id=run_id, status="completed")
        failed: set[str] = set()
        completed: set[str] = set()
        try:
            context = SourceContext(
                paths=self.paths,
                catalog=self.catalog,
                study_area=self.study_area,
                environment=self.environment,
                run_id=run_id,
            )
            for stage in normalized_stages:
                if stage is Stage.INVENTORY:
                    self._run_inventory(context)
                    continue
                if stage is Stage.BOOTSTRAP_ADMIN:
                    self._run_bootstrap(context, summary, failed, completed, resolve_only)
                    continue
                if stage is Stage.AOI:
                    self._run_aoi(context, summary)
                    continue
                if stage is Stage.HARMONIZE and not selected:
                    self._run_legacy_hydro(context, summary)
                    continue
                for source_id in selected:
                    if source_id in failed:
                        continue
                    try:
                        self._run_source_stage(stage, source_id, context, summary, resolve_only)
                    except _CONFIGURATION_ERRORS:
                        raise
                    except Exception:  # noqa: BLE001 - independent source failures are retained.
                        failed.add(source_id)
                        summary.errors[source_id] = f"{stage.value}_failed"
                    else:
                        completed.add(source_id)
        except Exception:
            summary.status = "failed"
            raise
        finally:
            summary.failed_sources = sorted(failed)
            summary.completed_sources = sorted(completed - failed)
            if summary.status != "failed":
                if failed and summary.completed_sources:
                    summary.status = "partial_failure"
                elif failed:
                    summary.status = "failed"
                else:
                    summary.status = "completed"
            self.catalog.end_run(run_id, summary.status, datetime.now(UTC))
        return summary

    def _selected_sources(self, source_ids: Sequence[str] | None) -> list[str]:
        selected = list(source_ids) if source_ids else sorted(self.source_specs)
        unknown = sorted(set(selected) - self.source_specs.keys())
        if unknown:
            raise ValueError(f"unknown source(s): {', '.join(unknown)}")
        return selected

    def _assets(self) -> list[AssetRecord]:
        return self.catalog._read_assets()

    def _run_inventory(self, context: SourceContext) -> None:
        inventory_existing(context)

    def _run_aoi(self, context: SourceContext, summary: RunSummary) -> None:
        """Compose the approved AOIs after administration is available and before downloads resolve."""
        import geopandas as gpd

        from flashflood_data.static.harmonize.aoi import build_study_areas, write_study_areas
        from flashflood_data.static.harmonize.hydro import select_basins_with_upstream

        core_path = self.paths.harmonized / "aoi" / "core_aoi.geoparquet"
        vietnam_path = self.paths.harmonized / "admin" / "vietnam_boundary.geoparquet"
        if not core_path.is_file() or not vietnam_path.is_file():
            raise ValueError("core AOI and Vietnam boundary are required before AOI composition")
        core_layer = gpd.read_parquet(core_path)
        vietnam_layer = gpd.read_parquet(vietnam_path).to_crs(core_layer.crs)
        if core_layer.empty or vietnam_layer.empty or core_layer.crs is None:
            raise ValueError("administrative AOI inputs are empty or missing a CRS")
        core = core_layer.geometry.union_all()
        vietnam = vietnam_layer.geometry.union_all()
        level = self.study_area.hydrobasins_level
        basin_path = (
            self.paths.dataset / "hybas_as_lev01-12_v1c" / f"hybas_as_lev{level:02d}_v1c.shp"
        )
        basins = gpd.read_file(basin_path)
        selected = select_basins_with_upstream(
            basins, core, hops=self.study_area.upstream_hops
        )
        areas = build_study_areas(core, selected, vietnam, self.study_area)
        write_study_areas(
            areas,
            self.paths.harmonized / "aoi",
            self.study_area.storage_crs,
            include_core=False,
        )
        summary.metrics.update({f"selected_l{level}": len(selected)})
        self._run_handler(Stage.AOI, "aoi", context, summary)

    def _run_bootstrap(
        self,
        context: SourceContext,
        summary: RunSummary,
        failed: set[str],
        completed: set[str],
        resolve_only: bool,
    ) -> None:
        for source_id in _BOOTSTRAP_SOURCES:
            if source_id not in self.source_specs:
                continue
            try:
                self._run_source_stage(Stage.FETCH, source_id, context, summary, resolve_only)
                if not resolve_only:
                    self._run_source_stage(Stage.VALIDATE, source_id, context, summary, False)
                    self._run_source_stage(Stage.HARMONIZE, source_id, context, summary, False)
            except _CONFIGURATION_ERRORS:
                raise
            except Exception:  # noqa: BLE001 - bootstrap sources remain independently recoverable.
                failed.add(source_id)
                summary.errors[source_id] = "bootstrap_failed"
            else:
                completed.add(source_id)

    def _run_source_stage(
        self,
        stage: Stage,
        source_id: str,
        context: SourceContext,
        summary: RunSummary,
        resolve_only: bool,
    ) -> None:
        if stage is Stage.FETCH:
            self._fetch_source(source_id, context, summary, resolve_only)
        elif stage is Stage.VALIDATE:
            self._validate_source(source_id, summary)
        elif stage is Stage.HARMONIZE:
            self._harmonize_source(source_id, context, summary)
        elif stage is Stage.DERIVE or stage is Stage.MAP or stage is Stage.QA:
            self._run_handler(stage, source_id, context, summary)
        else:
            raise ValueError(f"unsupported source stage: {stage.value}")

    def _fetch_source(
        self, source_id: str, context: SourceContext, summary: RunSummary, resolve_only: bool
    ) -> None:
        adapter = self.adapter_factory(self.source_specs[source_id])
        while True:
            remotes = adapter.resolve(context, self._available_for_resolution(source_id))
            pending = [remote for remote in remotes if not self._remote_is_reusable(remote)]
            if not pending:
                if remotes or any(
                    self._raw_matches_spec(asset, source_id) for asset in self._assets()
                ):
                    summary.reused += 1
                return
            for remote in pending:
                if resolve_only:
                    self._preflight(remote)
                else:
                    record = self._fetch_remote(adapter, context, remote)
                    if record.status is not AssetStatus.FETCHED:
                        raise ValueError("fetcher returned a non-fetched asset")
                    summary.fetched += 1
            if resolve_only:
                return

    def _available_for_resolution(self, source_id: str) -> list[AssetRecord]:
        return [
            asset
            for asset in self._assets()
            if asset.source_id != source_id
            or asset.kind is not AssetKind.RAW
            or self._raw_matches_spec(asset, source_id)
        ]

    def _raw_matches_spec(self, asset: AssetRecord, source_id: str) -> bool:
        spec = self.source_specs[source_id]
        return (
            asset.source_id == spec.source_id
            and asset.source_version == spec.version
            and asset.kind is AssetKind.RAW
            and asset.status in {AssetStatus.FETCHED, AssetStatus.VALIDATED}
            and self._checksum_matches(asset)
        )

    def _remote_is_reusable(self, remote: RemoteAsset) -> bool:
        candidates = [asset for asset in self._assets() if asset.asset_id == remote.asset_id]
        for candidate in candidates:
            if (
                candidate.status is AssetStatus.FAILED
                and self._remote_identity_matches(candidate, remote)
                and self._checksum_matches(candidate)
            ):
                self.catalog.transition(candidate.asset_id, AssetStatus.FETCHING)
                self.catalog.transition(
                    candidate.asset_id,
                    AssetStatus.FETCHED,
                    error_code=None,
                    error_message=None,
                )
        candidates = [asset for asset in self._assets() if asset.asset_id == remote.asset_id]
        reusable = any(self._remote_matches(asset, remote) for asset in candidates)
        if not reusable:
            for candidate in candidates:
                self._mark_stale(candidate)
        return reusable

    def _remote_matches(self, asset: AssetRecord, remote: RemoteAsset) -> bool:
        return (
            self._remote_identity_matches(asset, remote)
            and asset.status in {AssetStatus.FETCHED, AssetStatus.VALIDATED}
            and self._checksum_matches(asset)
        )

    def _remote_identity_matches(self, asset: AssetRecord, remote: RemoteAsset) -> bool:
        return (
            asset.source_id == remote.source_id
            and asset.source_version == remote.source_version
            and asset.kind is AssetKind.RAW
            and asset.source_uri == remote.uri
            and Path(asset.storage_path) == self.paths.dataset / remote.target_relative_path
            and asset.media_type == remote.media_type
            and asset.license_id == remote.license_id
            and asset.source_valid_time == remote.source_valid_time
            and self._remote_request_matches(asset, remote)
        )

    @staticmethod
    def _remote_request_matches(asset: AssetRecord, remote: RemoteAsset) -> bool:
        """Compare transport semantics retained in catalog metadata without admitting secrets."""
        try:
            metadata = json.loads(asset.metadata_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        return metadata == {
            "budget_size_bytes": remote.budget_size_bytes,
            "expected_checksum": remote.expected_checksum,
            "expected_size": remote.expected_size,
            "request_form": dict(remote.request_form),
            "request_method": remote.request_method,
        }

    def _fetch_remote(
        self, adapter: SourceAdapter, context: SourceContext, remote: RemoteAsset
    ) -> AssetRecord:
        fetch_raw = getattr(adapter, "fetch_raw", None)
        if callable(fetch_raw):
            return fetch_raw(self.fetcher, context, remote)
        return self.fetcher.fetch(remote, context.run_id)  # type: ignore[attr-defined,no-any-return]

    def _preflight(self, remote) -> None:
        size = remote.expected_size or remote.budget_size_bytes
        if size is None:
            raise BudgetRejected("missing_download_size_bound")
        decision = StorageBudget.from_config(self.paths.dataset, self.study_area).preflight(
            size, size
        )
        if not decision.allowed:
            raise BudgetRejected(decision.reason)

    def _validate_source(self, source_id: str, summary: RunSummary) -> None:
        adapter = self.adapter_factory(self.source_specs[source_id])
        records = [
            item
            for item in self._assets()
            if item.source_id == source_id and item.kind is AssetKind.RAW
        ]
        for record in records:
            if record.status is AssetStatus.VALIDATED and self._checksum_matches(record):
                summary.reused += 1
                continue
            if record.status is AssetStatus.VALIDATED:
                self._mark_stale(record)
                continue
            if record.status is not AssetStatus.FETCHED:
                continue
            result = adapter.validate_raw(Path(record.storage_path))
            if not result.passed:
                self.catalog.transition(
                    record.asset_id,
                    AssetStatus.FAILED,
                    error_code="raw_validation_failed",
                    error_message="raw payload validation failed",
                )
                raise ValueError("raw payload validation failed")
            self.catalog.transition(record.asset_id, AssetStatus.VALIDATED)
            summary.validated += 1

    def _harmonize_source(
        self, source_id: str, context: SourceContext, summary: RunSummary
    ) -> None:
        self._ensure_validated(source_id, summary)
        if source_id in _HYDRO_SOURCES:
            if source_id != _HYDRO_COMPOSITE_OWNER:
                return
            for dependency_source_id in sorted(_HYDRO_SOURCES - {source_id}):
                if dependency_source_id not in self.source_specs:
                    raise ValueError(
                        f"hydro composite is missing source spec: {dependency_source_id}"
                    )
                self._ensure_validated(dependency_source_id, summary)
            fingerprint = self._hydro_fingerprint()
        else:
            fingerprint = self._source_fingerprint(source_id, Stage.HARMONIZE)
        existing = [
            item
            for item in self._assets()
            if item.source_id == source_id
            and item.source_version == self.source_specs[source_id].version
            and item.kind is not AssetKind.RAW
            and not item.asset_id.startswith(("task15-", "task16-", "task17-"))
            and item.dependency_fingerprint == fingerprint
            and item.status in {AssetStatus.HARMONIZED, AssetStatus.DERIVED}
            and self._checksum_matches(item)
        ]
        output_candidates = [
            item
            for item in self._assets()
            if item.source_id == source_id
            and item.kind is not AssetKind.RAW
            and not item.asset_id.startswith(("task15-", "task16-", "task17-"))
        ]
        if existing and len(existing) == len(output_candidates):
            summary.reused += 1
            return
        for output in output_candidates:
            if output not in existing:
                self._mark_stale(output)
        if source_id == _HYDRO_COMPOSITE_OWNER:
            outputs = self._harmonize_hydro(context)
        else:
            adapter = self.adapter_factory(self.source_specs[source_id])
            outputs = adapter.harmonize(context, self._assets())
        for output in outputs:
            if output.kind is AssetKind.RAW:
                continue
            self.catalog.upsert(output.model_copy(update={"dependency_fingerprint": fingerprint}))
        summary.harmonized += len(
            [output for output in outputs if output.kind is not AssetKind.RAW]
        )

    def _run_legacy_hydro(self, context: SourceContext, summary: RunSummary) -> None:
        """Keep the established standalone hydro command on the shared orchestrator path."""
        outputs = self._harmonize_hydro(context)
        for output in outputs:
            self.catalog.upsert(output)
        summary.harmonized += len(outputs)
        summary.metrics.update(self._latest_hydro_metrics)

    def _ensure_validated(self, source_id: str, summary: RunSummary) -> None:
        records = [
            item
            for item in self._assets()
            if item.source_id == source_id and item.kind is AssetKind.RAW
        ]
        if any(item.status is AssetStatus.FETCHED for item in records):
            self._validate_source(source_id, summary)
            records = [
                item
                for item in self._assets()
                if item.source_id == source_id and item.kind is AssetKind.RAW
            ]
        invalid = [item for item in records if item.status is not AssetStatus.VALIDATED]
        if invalid:
            raise ValueError("harmonization requires validated raw assets")

    def _harmonize_hydro(self, context: SourceContext) -> list[AssetRecord]:
        """Compose the legacy hydro implementation without duplicating its lifecycle."""
        import geopandas as gpd

        from flashflood_data.static.harmonize.aoi import build_study_areas
        from flashflood_data.static.harmonize.hydro import (
            default_hydro_inputs,
            harmonize_hydro,
            select_l10_with_upstream,
        )

        core_path = self.paths.harmonized / "aoi" / "core_aoi.geoparquet"
        vietnam_path = self.paths.harmonized / "admin" / "vietnam_boundary.geoparquet"
        if not core_path.is_file() or not vietnam_path.is_file():
            raise ValueError("core AOI and Vietnam boundary are required before hydrology")
        core_layer = gpd.read_parquet(core_path)
        vietnam_layer = gpd.read_parquet(vietnam_path).to_crs(core_layer.crs)
        core = core_layer.geometry.union_all()
        vietnam = vietnam_layer.geometry.union_all()
        inputs = default_hydro_inputs(self.paths)
        l10 = gpd.read_file(inputs.l10)
        selected = select_l10_with_upstream(l10, core, hops=self.study_area.upstream_hops)
        intersecting = int(l10.geometry.intersects(core).sum())
        self._latest_hydro_metrics = {
            "intersecting_l10": intersecting,
            "selected_l10": len(selected),
            "upstream_l10": len(selected) - intersecting,
        }
        outputs = harmonize_hydro(
            self.paths,
            build_study_areas(core, selected, vietnam, self.study_area),
            inputs=inputs,
            hops=self.study_area.upstream_hops,
        )
        return [
            AssetRecord(
                asset_id=f"hydro-harmonized-{path.stem}",
                source_id="hydrobasins_v1c",
                source_version=self._hydro_spec().version,
                kind=AssetKind.HARMONIZED,
                source_uri="generated:hydro-harmonization",
                storage_path=str(path),
                media_type="application/geoparquet",
                size_bytes=path.stat().st_size,
                checksum=sha256_file(path),
                retrieved_at=datetime.now(UTC),
                license_id=self._hydro_spec().license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.HARMONIZED,
            )
            for path in outputs
        ]

    def _hydro_spec(self) -> SourceSpec:
        return self.source_specs.get(
            "hydrobasins_v1c",
            SourceSpec(
                source_id="hydrobasins_v1c",
                adapter="existing",
                version="1c",
                license_id="HydroSHEDS-free-academic",
            ),
        )

    def _run_handler(
        self, stage: Stage, source_id: str, context: SourceContext, summary: RunSummary
    ) -> None:
        handler = self._stage_handlers.get(stage)
        if handler is None:
            return
        outputs = handler(self, stage, source_id, context)
        for output in outputs:
            self.catalog.upsert(output)
        if stage is Stage.DERIVE:
            summary.derived += len(outputs)

    def _source_fingerprint(self, source_id: str, stage: Stage) -> str:
        source = self.source_specs[source_id]
        checksums = [
            asset.checksum
            for asset in self._assets()
            if asset.source_id == source_id and asset.kind is AssetKind.RAW
        ]
        if source_id == "geofabrik_vietnam_snapshot" and stage is Stage.HARMONIZE:
            checksums.append(sha256_file(self.paths.root / "config" / "osmconf.ini"))
        config = {
            "stage": stage.value,
            "source_version": source.version,
            "study_area": self.study_area.model_dump(mode="json"),
            "source_settings": source.settings,
        }
        return dependency_fingerprint(checksums, config, _PROCESSOR_VERSION)

    def _hydro_fingerprint(self) -> str:
        """Fingerprint the one composite from every validated hydro raw family."""
        raw_assets = sorted(
            (
                asset
                for asset in self._assets()
                if asset.source_id in _HYDRO_SOURCES
                and asset.kind is AssetKind.RAW
                and asset.status is AssetStatus.VALIDATED
            ),
            key=lambda asset: (asset.source_id, asset.asset_id),
        )
        present = {asset.source_id for asset in raw_assets}
        if present != _HYDRO_SOURCES:
            missing = sorted(_HYDRO_SOURCES - present)
            raise ValueError(f"hydro composite is missing validated raw families: {missing}")
        config = {
            "stage": Stage.HARMONIZE.value,
            "study_area": self.study_area.model_dump(mode="json"),
            "sources": {
                source_id: self.source_specs[source_id].model_dump(mode="json")
                for source_id in sorted(_HYDRO_SOURCES)
            },
            "raw_assets": [
                {
                    "asset_id": asset.asset_id,
                    "checksum": asset.checksum,
                    "source_id": asset.source_id,
                }
                for asset in raw_assets
            ],
        }
        return dependency_fingerprint(
            (asset.checksum for asset in raw_assets), config, _PROCESSOR_VERSION
        )

    def _mark_stale(self, record: AssetRecord) -> None:
        if record.status in {
            AssetStatus.VALIDATED,
            AssetStatus.FETCHED,
            AssetStatus.HARMONIZED,
            AssetStatus.DERIVED,
        }:
            self.catalog.transition(record.asset_id, AssetStatus.STALE)

    def _checksum_matches(self, record: AssetRecord) -> bool:
        path = Path(record.storage_path)
        if not path.is_file() or record.checksum_algorithm != "sha256":
            return False
        try:
            metadata = json.loads(record.metadata_json or "{}")
            relatives = metadata.get("bundle_members")
            if not isinstance(relatives, list) or not relatives:
                return sha256_file(path) == record.checksum
            dataset = self.paths.dataset.resolve(strict=True)
            members: list[tuple[str, str]] = []
            for relative in relatives:
                if not isinstance(relative, str):
                    return False
                member = (dataset / relative).resolve(strict=True)
                if not member.is_relative_to(dataset) or not member.is_file():
                    return False
                suffix = (
                    f"{path.suffix}.xml"
                    if member.name == f"{path.name}.xml"
                    else member.suffix
                )
                members.append((suffix, sha256_file(member)))
            return compound_checksum(members) == record.checksum
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return False
