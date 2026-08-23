from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from flashflood_data.catalog import AssetCatalog, sha256_file
from flashflood_data.models import (
    AssetKind,
    AssetRecord,
    AssetStatus,
    RemoteAsset,
    SourceSpec,
    ValidationResult,
)
from flashflood_data.pipeline import Stage, StaticPipeline
from flashflood_data.sources.base import SourceAdapter, SourceContext


class RecoveryAdapter(SourceAdapter):
    def resolve(self, context: SourceContext, available: list[AssetRecord]) -> list[RemoteAsset]:
        if any(item.asset_id == f"{self.spec.source_id}-raw" for item in available):
            return []
        return [
            RemoteAsset(
                asset_id=f"{self.spec.source_id}-raw",
                source_id=self.spec.source_id,
                source_version=self.spec.version,
                uri=f"https://example.invalid/{self.spec.source_id}",
                target_relative_path=Path(f"raw/{self.spec.source_id}.bin"),
                media_type="application/octet-stream",
                license_id=self.spec.license_id,
                expected_size=2,
            )
        ]

    def validate_raw(self, path: Path) -> ValidationResult:
        return ValidationResult(passed=True, checks={"fixture": True})

    def harmonize(self, context: SourceContext, assets: list[AssetRecord]) -> list[AssetRecord]:
        return []


class RecoveringFetcher:
    def __init__(self, catalog: AssetCatalog, paths) -> None:
        self.catalog = catalog
        self.paths = paths

    def fetch(self, remote: RemoteAsset, run_id: str) -> AssetRecord:
        target = self.paths.dataset / remote.target_relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        if remote.source_id == "bad":
            partial = Path(f"{target}.partial")
            partial.write_bytes(b"x")
            partial.unlink()
            raise RuntimeError("simulated interrupted download")
        target.write_bytes(b"ok")
        return self.catalog.upsert(
            AssetRecord(
                asset_id=remote.asset_id,
                source_id=remote.source_id,
                source_version=remote.source_version,
                kind=AssetKind.RAW,
                source_uri=remote.uri,
                storage_path=str(target),
                media_type=remote.media_type,
                size_bytes=target.stat().st_size,
                checksum=sha256_file(target),
                retrieved_at=datetime.now(UTC),
                license_id=remote.license_id,
                pipeline_run_id=run_id,
                status=AssetStatus.FETCHED,
            )
        )


def test_network_failure_leaves_good_asset_and_no_partial(project_paths) -> None:
    catalog = AssetCatalog(project_paths)
    specs = {
        source_id: SourceSpec(
            source_id=source_id, adapter="fixture", version="1", license_id="fixture"
        )
        for source_id in ("good", "bad")
    }
    pipeline = StaticPipeline(
        project_paths,
        source_specs=specs,
        adapter_factory=RecoveryAdapter,
        fetcher=RecoveringFetcher(catalog, project_paths),
    )

    summary = pipeline.run([Stage.FETCH], ["good", "bad"])

    assert summary.status == "partial_failure"
    assert (project_paths.raw / "good.bin").read_bytes() == b"ok"
    assert not (project_paths.raw / "bad.bin.partial").exists()
