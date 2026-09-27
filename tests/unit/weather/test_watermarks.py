from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from flashflood_data.orchestration.weather.models import (
    IngestWatermark,
    ObjectLifecycleRow,
    WeatherRasterSlice,
)
from flashflood_data.orchestration.weather.watermarks import IngestWatermarkStore


class MemoryMetaStore:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str, str, str], dict[str, object]] = {}

    def get_meta_row(self, identifier, key):
        assert identifier == ("meta", "ingest_watermarks")
        return self.rows.get(
            (
                key["source_id"],
                key["product"],
                key["stream_id"],
                key["spatial_scope_id"],
            )
        )

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
        spatial_scope_id="sonla-l12-h1",
        cursor_time=datetime(2026, 9, 1, tzinfo=UTC),
        last_safe_end=datetime(2026, 9, 2, tzinfo=UTC),
        last_run_id="run-1",
        status="gap",
        updated_at=datetime(2026, 9, 2, tzinfo=UTC),
        detail_json='{"missing":1}',
    )

    assert store.save(watermark) == 42
    assert store.load(
        "gsmap", "gauge_standard_v8", "gauge_standard", "sonla-l12-h1"
    ) == watermark
    assert store.load(
        "gsmap", "gauge_standard_v8", "gauge_standard", "laocai-l12-h1"
    ) is None


def test_weather_slice_rejects_shifted_or_duplicate_cell_values() -> None:
    values = {
        "slice_id": "slice-1",
        "object_id": "object-1",
        "source_id": "gsmap_now",
        "source_product": "gauge_now_v8",
        "source_grid_version": "grid-v1",
        "spatial_scope_id": "sonla-l12-h1",
        "variable": "precipitation",
        "vertical_level": "surface",
        "source_cycle_id": "20260927T0000Z",
        "valid_time": datetime(2026, 9, 27, 1, tzinfo=UTC),
        "window_start": datetime(2026, 9, 27, 0, tzinfo=UTC),
        "window_end": datetime(2026, 9, 27, 1, tzinfo=UTC),
        "source_revision": 0,
        "cell_indices": (3, 8),
        "values": (1.5, 2.5),
        "unit": "mm",
        "value_kind": "accumulation",
        "available_at": datetime(2026, 9, 27, 2, tzinfo=UTC),
        "ingest_run_id": "run-1",
        "parser_version": "v1",
        "quality_status": "passed",
    }

    assert WeatherRasterSlice.model_validate(values).cell_indices == (3, 8)
    with pytest.raises(ValidationError, match="same length"):
        WeatherRasterSlice.model_validate({**values, "values": (1.5,)})
    with pytest.raises(ValidationError, match="sorted and unique"):
        WeatherRasterSlice.model_validate({**values, "cell_indices": (8, 3)})


def test_object_lifecycle_requires_expiry_only_for_transient_objects() -> None:
    values = {
        "object_id": "object-1",
        "retention_class": "transient_7d",
        "storage_status": "available",
        "expires_at": datetime(2026, 10, 4, tzinfo=UTC),
        "bronze_snapshot_id": None,
        "quality_status": "pending",
        "lineage_edge_id": None,
        "deleted_at": None,
        "last_checked_at": datetime(2026, 9, 27, tzinfo=UTC),
        "reason": None,
    }

    assert ObjectLifecycleRow.model_validate(values).expires_at is not None
    with pytest.raises(ValidationError, match="expires_at"):
        ObjectLifecycleRow.model_validate({**values, "expires_at": None})
    with pytest.raises(ValidationError, match="cannot expire"):
        ObjectLifecycleRow.model_validate(
            {**values, "retention_class": "durable", "expires_at": values["expires_at"]}
        )
