"""Pipeline handler for static quality assurance."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flashflood_data.catalog import sha256_file
from flashflood_data.catalog.models import AssetKind, AssetRecord, AssetStatus
from flashflood_data.static.qa.models import QualityGateFailure
from flashflood_data.static.qa.runner import run_quality_gates


def task17_qa_handler(
    *, owner_source_id: str
) -> Callable[[Any, object, str, Any], list[AssetRecord]]:
    """Return a concrete ``Stage.QA`` handler; Task 19 owns its registration."""

    def handler(pipeline: Any, stage: object, source_id: str, context: Any) -> list[AssetRecord]:
        if str(stage) != "qa":
            raise ValueError("Task 17 handler can only run at the qa stage")
        if source_id != owner_source_id:
            return []
        from flashflood_data.static.qa.map import QA_MAP_BUNDLE_RELATIVE_PATHS, publish_qa_map
        from flashflood_data.static.qa.report import publish_report

        report = run_quality_gates(pipeline.paths, context.study_area)
        publish_qa_map(pipeline.paths, pipeline.paths.qa)
        outputs = [
            *publish_report(report, pipeline.paths.qa),
            *(pipeline.paths.qa / relative for relative in QA_MAP_BUNDLE_RELATIVE_PATHS),
        ]
        source = pipeline.source_specs[source_id]

        def asset_suffix(path: Path) -> str:
            relative = path.relative_to(pipeline.paths.qa)
            return "-".join((*relative.parent.parts, relative.stem, relative.suffix[1:]))

        def media_type(path: Path) -> str:
            return {
                ".geojson": "application/geo+json",
                ".html": "text/html",
                ".json": "application/json",
                ".parquet": "application/vnd.apache.parquet",
                ".png": "image/png",
            }[path.suffix]

        records = [
            AssetRecord(
                asset_id=f"task17-qa-{asset_suffix(path)}",
                source_id=source.source_id,
                source_version=source.version,
                kind=AssetKind.QA,
                source_uri="generated:task17-static-qa",
                storage_path=str(path),
                media_type=media_type(path),
                size_bytes=path.stat().st_size,
                checksum=sha256_file(path),
                retrieved_at=datetime.now(UTC),
                license_id=source.license_id,
                pipeline_run_id=context.run_id,
                status=AssetStatus.DERIVED,
            )
            for path in outputs
        ]
        for record in records:
            pipeline.catalog.upsert(record)
        if report.fatal_failures:
            raise QualityGateFailure("fatal quality gates failed after publishing QA artifacts")
        return records

    return handler
