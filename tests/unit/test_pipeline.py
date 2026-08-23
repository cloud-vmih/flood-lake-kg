from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import box

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.http import BudgetRejected
from flashflood_data.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    SourceSpec,
    ValidationResult,
)
from flashflood_data.pipeline import STATIC_ORDER, Stage, StaticPipeline, dependency_fingerprint
from flashflood_data.sources.base import SourceAdapter, SourceContext
from flashflood_data.sources.cop_dem import MissingCredentials


class FixtureAdapter(SourceAdapter):
    def __init__(self, spec: SourceSpec, *, broken: bool = False) -> None:
        super().__init__(spec)
        self.broken = broken

    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        if any(record.asset_id == f"{self.spec.source_id}-raw" for record in available):
            return []
        return [
            RemoteAsset(
                asset_id=f"{self.spec.source_id}-raw",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                uri=f"https://example.invalid/{self.spec.source_id}.bin",
                target_relative_path=Path(f"raw/{self.spec.source_id}-{self.spec.version}.bin"),
                media_type="application/octet-stream",
                license_id=self.spec.license_id,
                expected_size=7,
            )
        ]

    def validate_raw(self, path: Path) -> ValidationResult:
        return ValidationResult(passed=path.read_bytes() == b"fixture", checks={"fixture": True})

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        raw = next(record for record in assets if record.asset_id == f"{self.spec.source_id}-raw")
        output = context.paths.harmonized / f"{self.spec.source_id}.bin"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(Path(raw.storage_path).read_bytes())
        return [
            AssetRecord(
                asset_id=f"{self.spec.source_id}-harmonized",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                kind=AssetKind.HARMONIZED,
                source_uri="generated:fixture",
                storage_path=str(output),
                media_type="application/octet-stream",
                size_bytes=output.stat().st_size,
                checksum=sha256_file(output),
                retrieved_at=datetime.now(UTC),
                license_id=self.spec.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.HARMONIZED,
            )
        ]


class FixtureFetcher:
    def __init__(
        self, catalog: AssetCatalog, paths, *, broken_sources: set[str] | None = None
    ) -> None:
        self.catalog = catalog
        self.paths = paths
        self.broken_sources = broken_sources or set()
        self.calls: list[str] = []

    def fetch(self, remote: RemoteAsset, run_id: str) -> AssetRecord:
        self.calls.append(remote.source_id)
        if remote.source_id in self.broken_sources:
            raise RuntimeError("fixture download failure")
        path = self.paths.dataset / remote.target_relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture")
        record = AssetRecord(
            asset_id=remote.asset_id,
            source_id=remote.source_id,
            source_version=remote.source_version,
            kind=AssetKind.RAW,
            source_uri=remote.uri,
            storage_path=str(path),
            media_type=remote.media_type,
            size_bytes=path.stat().st_size,
            checksum=sha256_file(path),
            retrieved_at=datetime.now(UTC),
            license_id=remote.license_id,
            pipeline_run_id=run_id,
            status=AssetStatus.FETCHED,
        )
        return self.catalog.upsert(record)


@pytest.fixture
def fake_pipeline(project_paths) -> StaticPipeline:
    catalog = AssetCatalog(project_paths)
    spec = SourceSpec(source_id="source-a", adapter="fixture", version="1", license_id="fixture")
    return StaticPipeline(
        project_paths,
        source_specs={spec.source_id: spec},
        adapter_factory=lambda item: FixtureAdapter(item),
        fetcher=FixtureFetcher(catalog, project_paths),
    )


@pytest.fixture
def fake_pipeline_with_failure(project_paths) -> StaticPipeline:
    catalog = AssetCatalog(project_paths)
    specs = {
        name: SourceSpec(source_id=name, adapter="fixture", version="1", license_id="fixture")
        for name in ("good", "bad")
    }
    return StaticPipeline(
        project_paths,
        source_specs=specs,
        adapter_factory=lambda item: FixtureAdapter(item),
        fetcher=FixtureFetcher(catalog, project_paths, broken_sources={"bad"}),
    )


def test_fingerprint_is_order_independent() -> None:
    left = dependency_fingerprint(["b", "a"], {"level": 10}, "0.1.0")
    right = dependency_fingerprint(["a", "b"], {"level": 10}, "0.1.0")
    assert left == right


def test_unchanged_second_run_skips_fetch_and_build(fake_pipeline: StaticPipeline) -> None:
    first = fake_pipeline.run([Stage.FETCH, Stage.HARMONIZE], ["source-a"])
    second = fake_pipeline.run([Stage.FETCH, Stage.HARMONIZE], ["source-a"])

    assert first.fetched == 1 and first.harmonized == 1
    assert second.fetched == 0 and second.harmonized == 0 and second.reused == 2


def test_source_failure_keeps_independent_completed_assets(
    fake_pipeline_with_failure: StaticPipeline,
) -> None:
    summary = fake_pipeline_with_failure.run([Stage.FETCH], ["good", "bad"])

    assert summary.failed_sources == ["bad"]
    assert summary.completed_sources == ["good"]
    assert summary.status == "partial_failure"


class BootstrapAoiAdapter(FixtureAdapter):
    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        if self.spec.source_id == "downstream":
            for name in ("hydrological_aoi", "environmental_aoi", "exposure_aoi"):
                assert (context.paths.harmonized / "aoi" / f"{name}.geoparquet").is_file()
            return []
        return super().resolve(context, available)

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        if self.spec.source_id == "sonla_admin_2025":
            target = context.paths.harmonized / "aoi" / "core_aoi.geoparquet"
            geometry = box(103.01, 20.01, 103.09, 20.09)
        else:
            target = context.paths.harmonized / "admin" / "vietnam_boundary.geoparquet"
            geometry = box(103.0, 20.0, 103.15, 20.15)
        target.parent.mkdir(parents=True, exist_ok=True)
        gpd.GeoDataFrame(geometry=[geometry], crs="EPSG:4326").to_parquet(target, index=False)
        return [
            AssetRecord(
                asset_id=f"{self.spec.source_id}-harmonized",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                kind=AssetKind.HARMONIZED,
                source_uri="generated:fixture-admin",
                storage_path=str(target),
                media_type="application/geoparquet",
                size_bytes=target.stat().st_size,
                checksum=sha256_file(target),
                retrieved_at=datetime.now(UTC),
                license_id=self.spec.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.HARMONIZED,
            )
        ]


def test_bootstrap_then_aoi_writes_downstream_aoi_inputs(project_paths: Path) -> None:
    fixture = Path("tests/fixtures/hydro/basins_l10.geojson")
    raw_dir = project_paths.dataset / "hybas_as_lev01-12_v1c"
    raw_dir.mkdir(parents=True)
    gpd.read_file(fixture).to_file(raw_dir / "hybas_as_lev10_v1c.shp")
    catalog = AssetCatalog(project_paths)
    specs = {
        source_id: SourceSpec(
            source_id=source_id, adapter="fixture", version="1", license_id="fixture"
        )
        for source_id in ("sonla_admin_2025", "gadm_vnm_4_1", "downstream")
    }
    pipeline = StaticPipeline(
        project_paths,
        source_specs=specs,
        adapter_factory=BootstrapAoiAdapter,
        fetcher=FixtureFetcher(catalog, project_paths),
    )

    summary = pipeline.run([Stage.BOOTSTRAP_ADMIN, Stage.AOI, Stage.FETCH])

    assert summary.status == "completed"
    for name in ("hydrological_aoi", "environmental_aoi", "exposure_aoi"):
        assert (project_paths.harmonized / "aoi" / f"{name}.geoparquet").is_file()


def test_corrupt_raw_asset_is_not_reused(fake_pipeline: StaticPipeline) -> None:
    fake_pipeline.run([Stage.FETCH], ["source-a"])
    raw = fake_pipeline.paths.raw / "source-a-1.bin"
    raw.write_bytes(b"corrupt")

    summary = fake_pipeline.run([Stage.FETCH], ["source-a"])

    assert summary.fetched == 1


def test_source_version_change_is_not_reused(project_paths) -> None:
    catalog = AssetCatalog(project_paths)
    v1 = SourceSpec(source_id="source-a", adapter="fixture", version="1", license_id="fixture")
    first = StaticPipeline(
        project_paths,
        source_specs={v1.source_id: v1},
        adapter_factory=FixtureAdapter,
        fetcher=FixtureFetcher(catalog, project_paths),
    )
    first.run([Stage.FETCH, Stage.HARMONIZE], ["source-a"])
    v2 = v1.model_copy(update={"version": "2"})
    second = StaticPipeline(
        project_paths,
        source_specs={v2.source_id: v2},
        adapter_factory=FixtureAdapter,
        fetcher=FixtureFetcher(catalog, project_paths),
    )

    summary = second.run([Stage.FETCH, Stage.HARMONIZE], ["source-a"])

    assert summary.fetched == 1
    assert summary.harmonized == 1


class StaleObservingAdapter(FixtureAdapter):
    def __init__(self, spec: SourceSpec, observed: list[AssetStatus]) -> None:
        super().__init__(spec)
        self.observed = observed

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        self.observed.extend(
            asset.status
            for asset in assets
            if asset.asset_id == f"{self.spec.source_id}-harmonized"
        )
        return super().harmonize(context, assets)


def test_corrupt_harmonized_output_is_marked_stale_before_rebuild(project_paths) -> None:
    catalog = AssetCatalog(project_paths)
    spec = SourceSpec(source_id="source-a", adapter="fixture", version="1", license_id="fixture")
    first = StaticPipeline(
        project_paths,
        source_specs={spec.source_id: spec},
        adapter_factory=FixtureAdapter,
        fetcher=FixtureFetcher(catalog, project_paths),
    )
    first.run([Stage.FETCH, Stage.HARMONIZE], ["source-a"])
    (project_paths.harmonized / "source-a.bin").write_bytes(b"corrupt")
    observed: list[AssetStatus] = []
    second = StaticPipeline(
        project_paths,
        source_specs={spec.source_id: spec},
        adapter_factory=lambda item: StaleObservingAdapter(item, observed),
        fetcher=FixtureFetcher(catalog, project_paths),
    )

    summary = second.run([Stage.HARMONIZE], ["source-a"])

    assert summary.harmonized == 1
    assert observed == [AssetStatus.STALE]


class AuthFetchingAdapter(FixtureAdapter):
    def __init__(self, spec: SourceSpec, delegated: list[RemoteAsset]) -> None:
        super().__init__(spec)
        self.delegated = delegated

    def fetch_raw(
        self, fetcher: FixtureFetcher, context: SourceContext, remote: RemoteAsset
    ) -> AssetRecord:
        self.delegated.append(remote)
        return fetcher.fetch(remote, context.run_id)


def test_authenticated_adapter_fetch_raw_is_used_instead_of_generic_fetch(project_paths) -> None:
    catalog = AssetCatalog(project_paths)
    spec = SourceSpec(source_id="source-a", adapter="fixture", version="1", license_id="fixture")
    delegated: list[RemoteAsset] = []
    pipeline = StaticPipeline(
        project_paths,
        source_specs={spec.source_id: spec},
        adapter_factory=lambda item: AuthFetchingAdapter(item, delegated),
        fetcher=FixtureFetcher(catalog, project_paths),
    )

    pipeline.run([Stage.FETCH], ["source-a"])

    assert [remote.asset_id for remote in delegated] == ["source-a-raw"]


class CredentialsAdapter(FixtureAdapter):
    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        raise MissingCredentials("CDSE credentials missing")


def test_missing_credentials_escape_per_source_failure_handling(project_paths) -> None:
    spec = SourceSpec(source_id="source-a", adapter="fixture", version="1", license_id="fixture")
    pipeline = StaticPipeline(
        project_paths,
        source_specs={spec.source_id: spec},
        adapter_factory=CredentialsAdapter,
    )

    with pytest.raises(MissingCredentials):
        pipeline.run([Stage.FETCH], ["source-a"])


class BudgetAdapter(FixtureAdapter):
    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        raise BudgetRejected("minimum_free_space")


def test_budget_rejection_escapes_per_source_failure_handling(project_paths) -> None:
    spec = SourceSpec(source_id="source-a", adapter="fixture", version="1", license_id="fixture")
    pipeline = StaticPipeline(
        project_paths,
        source_specs={spec.source_id: spec},
        adapter_factory=BudgetAdapter,
    )

    with pytest.raises(BudgetRejected):
        pipeline.run([Stage.FETCH], ["source-a"])


def test_static_order_has_independent_map_stage() -> None:
    assert [stage.value for stage in STATIC_ORDER][-3:] == ["derive", "map", "qa"]
