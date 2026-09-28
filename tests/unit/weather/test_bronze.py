import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.orchestration.weather.bronze import WeatherBronzeService


class Inventory:
    def __init__(self, *rows: SourceObjectRow) -> None:
        self.rows = rows

    def available_objects(self, source_id: str):
        return tuple(row for row in self.rows if source_id == row.source_id)


class ObjectStore:
    def __init__(self, payloads: bytes | dict[str, bytes]) -> None:
        self.payloads = (
            {"raw/weather/raw-1.json": payloads}
            if isinstance(payloads, bytes)
            else payloads
        )

    def download(self, key: str, local_path: Path) -> None:
        local_path.write_bytes(self.payloads[key])


class Writer:
    def __init__(self) -> None:
        self.rows = []
        self.commits = 0

    def published_weather_objects(self, parser_version: str):
        return {
            row["object_id"] for row in self.rows if row["parser_version"] == parser_version
        }

    def replace_object_batches(self, identifier, object_id, batches):
        assert identifier == ("bronze", "weather_raster_slice")
        incoming = [row for batch in batches for row in batch]
        assert all(row["object_id"] == object_id for row in incoming)
        self.rows = [row for row in self.rows if row["object_id"] != object_id] + incoming
        self.commits += 1
        return 16 + self.commits, len(incoming)

    def source_grid_indices(self, source_id, source_grid_version, spatial_scope_id):
        assert (source_id, source_grid_version, spatial_scope_id) == (
            "ifs_openmeteo", "grid-v1", "sonla-scope-v1"
        )
        return {121}


class Meta:
    meta_namespace = "meta"

    def __init__(self) -> None:
        self.runs = []
        self.lineage = []

    def record_run(self, row):
        self.runs.append(row)
        return 1

    def record_snapshot_ref(self, **row):
        return 1

    def record_quality(self, **row):
        return 1

    def record_lineage(self, **row):
        self.lineage.append(row)
        return "edge"


class Lifecycle:
    def __init__(self) -> None:
        self.evidence = []

    def expired_object_ids(self, object_ids):
        return set()

    def mark_bronze_evidence(
        self, object_id, snapshot_id, quality_status, lineage_edge_id, checked_at
    ):
        self.evidence.append(
            (object_id, snapshot_id, quality_status, lineage_edge_id, checked_at)
        )
        return 1


def _raw_row(
    payload: bytes, *, object_id: str = "raw-1", source_revision: int = 3
) -> SourceObjectRow:
    now = datetime(2026, 9, 1, 2, tzinfo=UTC)
    selection = {
        "stream_id": "hres_single_runs",
        "window_start": "2026-09-01T00:00:00Z",
        "window_end": "2026-09-01T06:00:00Z",
        "variables": ["precipitation"],
        "source_cycle_id": "20260901T0000Z",
        "source_revision": source_revision,
        "source_grid_version": "grid-v1",
        "spatial_scope_id": "sonla-scope-v1",
        "cell_count": 1,
        "options": {},
    }
    return SourceObjectRow(
        object_id=object_id,
        asset_id=f"cycle-{source_revision}",
        source_id="ifs_openmeteo",
        source_version="v1",
        source_type="dynamic",
        product="ecmwf_ifs",
        object_uri=f"s3://raw/weather/{object_id}.json",
        manifest_uri=f"s3://raw/weather/{object_id}.json.manifest.json",
        media_type="application/json",
        size_bytes=len(payload),
        checksum=sha256(payload).hexdigest(),
        source_uri="https://example.test",
        model_run_time="2026-09-01T00:00:00Z",
        valid_time="2026-09-01T00:00:00Z",
        available_at="2026-09-01T01:00:00Z",
        retrieved_at=now,
        first_seen_at=now,
        ingest_run_id="landing",
        selection_json=json.dumps(selection),
    )


def test_weather_bronze_discovers_then_parses_registered_raw_object(tmp_path: Path) -> None:
    payload = json.dumps(
        {
            "source_cycle_id": "20260901T0000Z",
            "model_run_time": "2026-09-01T00:00:00Z",
            "responses": [
                {
                    "latitude": 21.5,
                    "longitude": 104.0,
                    "cell_index": 121,
                    "hourly": {"time": ["2026-09-01T01:00"], "precipitation": [2.5]},
                    "hourly_units": {"precipitation": "mm"},
                }
            ],
        }
    ).encode()
    row = _raw_row(payload)
    writer = Writer()
    meta = Meta()
    lifecycle = Lifecycle()
    service = WeatherBronzeService(
        inventory=Inventory(row),
        object_store=ObjectStore(payload),
        writer=writer,
        meta=meta,
        lifecycle=lifecycle,
        raw_bucket="raw",
        staging_root=tmp_path,
    )

    assert service.discover("ifs_openmeteo", parser_version="ifs-v1") == ("raw-1",)
    result = service.process_object(
        "ifs_openmeteo", "raw-1", run_id="airflow-run", parser_version="ifs-v1"
    )

    assert result["status"] == "succeeded"
    assert result["row_count"] == 1
    assert writer.rows[0]["values"] == [2.5]
    assert writer.rows[0]["cell_indices"] == [121]
    assert service.discover("ifs_openmeteo", parser_version="ifs-v1") == ()
    assert meta.runs[-1]["status"] == "succeeded"
    assert meta.lineage[-1]["input_object_id"] == "raw-1"
    assert lifecycle.evidence[0][:4] == ("raw-1", 17, "passed", "edge")
    assert writer.commits == 1


def test_weather_bronze_rejects_negative_precipitation_before_commit(tmp_path: Path) -> None:
    payload = json.dumps(
        {
            "responses": [
                {
                    "latitude": 21.5,
                    "longitude": 104.0,
                    "cell_index": 121,
                    "hourly": {"time": ["2026-09-01T01:00"], "precipitation": [-1.0]},
                    "hourly_units": {"precipitation": "mm"},
                }
            ]
        }
    ).encode()
    row = _raw_row(payload)
    writer = Writer()
    meta = Meta()
    service = WeatherBronzeService(
        inventory=Inventory(row),
        object_store=ObjectStore(payload),
        writer=writer,
        meta=meta,
        lifecycle=Lifecycle(),
        raw_bucket="raw",
        staging_root=tmp_path,
    )

    with pytest.raises(ValueError, match="negative precipitation"):
        service.process_object(
            "ifs_openmeteo", "raw-1", run_id="airflow-run", parser_version="ifs-v1"
        )

    assert writer.rows == []
    assert meta.runs[-1]["status"] == "failed"


def test_revised_raw_object_remains_beside_previous_revision(tmp_path: Path) -> None:
    def payload(value: float) -> bytes:
        return json.dumps(
            {
                "responses": [
                    {
                        "latitude": 21.5,
                        "longitude": 104.0,
                        "cell_index": 121,
                        "hourly": {
                            "time": ["2026-09-01T01:00"],
                            "precipitation": [value],
                        },
                        "hourly_units": {"precipitation": "mm"},
                    }
                ]
            }
        ).encode()

    old_payload = payload(2.5)
    revised_payload = payload(3.5)
    old = _raw_row(old_payload, object_id="raw-1", source_revision=3)
    revised = _raw_row(revised_payload, object_id="raw-2", source_revision=4)
    writer = Writer()
    service = WeatherBronzeService(
        inventory=Inventory(old, revised),
        object_store=ObjectStore(
            {
                "raw/weather/raw-1.json": old_payload,
                "raw/weather/raw-2.json": revised_payload,
            }
        ),
        writer=writer,
        meta=Meta(),
        lifecycle=Lifecycle(),
        raw_bucket="raw",
        staging_root=tmp_path,
    )

    service.process_object(
        "ifs_openmeteo", "raw-1", run_id="run-old", parser_version="ifs-v1"
    )
    service.process_object(
        "ifs_openmeteo", "raw-2", run_id="run-new", parser_version="ifs-v1"
    )

    assert {row["object_id"] for row in writer.rows} == {"raw-1", "raw-2"}
    assert {row["source_revision"] for row in writer.rows} == {3, 4}
    assert writer.commits == 2
