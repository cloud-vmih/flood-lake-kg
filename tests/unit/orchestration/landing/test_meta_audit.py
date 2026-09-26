"""Raw registration in the existing landing pipeline is audited in Meta."""

from datetime import UTC, datetime

from flashflood_data.orchestration.landing.meta_audit import audit_registered_batch
from flashflood_data.orchestration.landing.models import RegisteredBatch, SourceObjectRow

NOW = datetime(2026, 9, 20, tzinfo=UTC)


class _Meta:
    def __init__(self) -> None:
        self.attempts = []
        self.quality = []
        self.snapshots = []
        self.runs = []

    def record_attempts(self, rows):
        self.attempts.extend(dict(row) for row in rows)

    def record_qualities(self, rows):
        self.quality.extend(dict(row) for row in rows)

    def record_snapshot_ref(self, **kwargs):
        self.snapshots.append(kwargs)

    def record_run(self, row):
        self.runs.append(dict(row))


def test_registered_raw_batch_records_attempt_quality_snapshot_and_run() -> None:
    row = SourceObjectRow(
        object_id="raw-1", asset_id="hydro-l12", source_id="hydrobasins_v1c",
        source_version="1c", product="hydrobasins_v1c", basin_level=12,
        object_uri="s3://raw/static/hydrobasins_v1c/hydro.zip",
        manifest_uri="s3://raw/static/hydrobasins_v1c/manifest.json",
        media_type="application/zip", size_bytes=123, checksum="a" * 64,
        source_uri="https://example.invalid/hydro", retrieved_at=NOW, first_seen_at=NOW,
        ingest_run_id="landing-1", selection_json='{"basin_level":12}',
    )
    batch = RegisteredBatch(
        source_id="hydrobasins_v1c", run_id="landing-1",
        object_ids=("raw-1",), snapshot_id=21,
    )
    meta = _Meta()
    audit_registered_batch(batch, [row], meta, catalog_name="flood_lakehouse", checked_at=NOW)
    assert meta.attempts[0]["asset_id"] == "hydro-l12"
    assert meta.quality[0]["status"] == "passed"
    assert meta.snapshots[0]["table_name"] == "flood_lakehouse.meta.source_objects"
    assert meta.snapshots[0]["iceberg_snapshot_id"] == 21
    assert meta.runs[0]["published_at"] == NOW


def test_registered_raw_batch_batches_per_object_meta_writes() -> None:
    rows = [
        SourceObjectRow(
            object_id=f"raw-{index}", asset_id=f"soil-{index}",
            source_id="soilgrids_2_0", source_version="2.0",
            product="soilgrids_2_0",
            object_uri=f"s3://raw/static/soilgrids_2_0/{index}.tif",
            manifest_uri=f"s3://raw/static/soilgrids_2_0/{index}/manifest.json",
            media_type="image/tiff", size_bytes=123, checksum=str(index) * 64,
            source_uri="https://example.invalid/soil", retrieved_at=NOW,
            first_seen_at=NOW, ingest_run_id="landing-1",
        )
        for index in (1, 2)
    ]
    batch = RegisteredBatch(
        source_id="soilgrids_2_0", run_id="landing-1",
        object_ids=tuple(row.object_id for row in rows), snapshot_id=22,
    )

    class CountingMeta(_Meta):
        def __init__(self) -> None:
            super().__init__()
            self.attempt_batches = 0
            self.quality_batches = 0

        def record_attempts(self, values):
            self.attempt_batches += 1
            super().record_attempts(values)

        def record_qualities(self, values):
            self.quality_batches += 1
            super().record_qualities(values)

    meta = CountingMeta()
    audit_registered_batch(batch, rows, meta, catalog_name="flood_lakehouse", checked_at=NOW)

    assert meta.attempt_batches == 1
    assert meta.quality_batches == 1
    assert len(meta.attempts) == 2
    assert len(meta.quality) == 2
