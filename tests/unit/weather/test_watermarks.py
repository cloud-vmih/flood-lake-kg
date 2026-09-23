from datetime import UTC, datetime

from flashflood_data.orchestration.weather.models import IngestWatermark
from flashflood_data.orchestration.weather.watermarks import IngestWatermarkStore


class MemoryMetaStore:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str, str], dict[str, object]] = {}

    def get_meta_row(self, identifier, key):
        assert identifier == ("meta", "ingest_watermarks")
        return self.rows.get((key["source_id"], key["product"], key["stream_id"]))

    def upsert_meta_row(self, identifier, key_fields, row):
        assert identifier == ("meta", "ingest_watermarks")
        key = tuple(row[name] for name in key_fields)
        self.rows[key] = dict(row)
        return 42


def test_watermark_round_trips_by_full_stream_identity() -> None:
    backend = MemoryMetaStore()
    store = IngestWatermarkStore(backend)
    watermark = IngestWatermark(
        source_id="gsmap",
        product="gauge_standard_v8",
        stream_id="gauge_standard",
        cursor_time=datetime(2026, 9, 1, tzinfo=UTC),
        last_safe_end=datetime(2026, 9, 2, tzinfo=UTC),
        last_run_id="run-1",
        status="gap",
        updated_at=datetime(2026, 9, 2, tzinfo=UTC),
        detail_json='{"missing":1}',
    )

    assert store.save(watermark) == 42
    assert store.load("gsmap", "gauge_standard_v8", "gauge_standard") == watermark
    assert store.load("gsmap", "gauge_now_v8", "gauge_now") is None
