from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    SourceSpec,
    ValidationResult,
)
from flashflood_data.pipeline import Stage, StaticPipeline, dependency_fingerprint
from flashflood_data.sources.base import SourceAdapter, SourceContext


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
                target_relative_path=Path(f"raw/{self.spec.source_id}.bin"),
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
