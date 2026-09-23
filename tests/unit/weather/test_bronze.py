import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import pytest

from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.orchestration.weather.bronze import WeatherBronzeService


class Inventory:
    def __init__(self, row: SourceObjectRow) -> None:
        self.row = row

    def available_objects(self, source_id: str):
        return (self.row,) if source_id == self.row.source_id else ()


class ObjectStore:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def download(self, key: str, local_path: Path) -> None:
        assert key == "raw/weather/ifs.json"
        local_path.write_bytes(self.payload)


class Writer:
    def __init__(self) -> None:
        self.rows = []

    def published_weather_objects(self, parser_version: str):
        return {
            row["object_id"] for row in self.rows if row["parser_version"] == parser_version
        }

    def replace_object_batches(self, identifier, object_id, batches):
        assert identifier == ("bronze", "weather_grid_value")
        self.rows = [row for batch in batches for row in batch]
        assert all(row["object_id"] == object_id for row in self.rows)
        return 17, len(self.rows)


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


def _raw_row(payload: bytes) -> SourceObjectRow:
    now = datetime(2026, 9, 1, 2, tzinfo=UTC)
    selection = {
        "stream_id": "hres_single_runs",
        "window_start": "2026-09-01T00:00:00Z",
        "window_end": "2026-09-01T06:00:00Z",
        "variables": ["precipitation"],
        "source_cycle_id": "20260901T0000Z",
        "source_revision": 3,
        "options": {},
    }
    return SourceObjectRow(
        object_id="raw-1",
        asset_id="cycle-1",
        source_id="ifs_openmeteo",
        source_version="v1",
        source_type="dynamic",
        product="ecmwf_ifs",
        object_uri="s3://raw/weather/ifs.json",
        manifest_uri="s3://raw/weather/ifs.json.manifest.json",
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
                    "hourly": {"time": ["2026-09-01T01:00"], "precipitation": [2.5]},
                    "hourly_units": {"precipitation": "mm"},
                }
            ],
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
        raw_bucket="raw",
        staging_root=tmp_path,
    )

    assert service.discover("ifs_openmeteo", parser_version="ifs-v1") == ("raw-1",)
    result = service.process_object(
        "ifs_openmeteo", "raw-1", run_id="airflow-run", parser_version="ifs-v1"
    )

    assert result["status"] == "succeeded"
    assert result["row_count"] == 1
    assert writer.rows[0]["value"] == 2.5
    assert service.discover("ifs_openmeteo", parser_version="ifs-v1") == ()
    assert meta.runs[-1]["status"] == "succeeded"
    assert meta.lineage[-1]["input_object_id"] == "raw-1"


def test_weather_bronze_rejects_negative_precipitation_before_commit(tmp_path: Path) -> None:
    payload = json.dumps(
        {
            "responses": [
                {
                    "latitude": 21.5,
                    "longitude": 104.0,
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
        raw_bucket="raw",
        staging_root=tmp_path,
    )

    with pytest.raises(ValueError, match="negative precipitation"):
        service.process_object(
            "ifs_openmeteo", "raw-1", run_id="airflow-run", parser_version="ifs-v1"
        )

    assert writer.rows == []
    assert meta.runs[-1]["status"] == "failed"
