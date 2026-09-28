import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import xarray as xr

from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.orchestration.weather.parsers import parse_weather_object


def _row(
    path: Path,
    *,
    source_id: str,
    product: str,
    selection: dict[str, object],
) -> SourceObjectRow:
    from hashlib import sha256

    payload = path.read_bytes()
    now = datetime(2026, 9, 1, 2, tzinfo=UTC)
    return SourceObjectRow(
        object_id="object-1",
        asset_id="asset-1",
        source_id=source_id,
        source_version="v1",
        source_type="dynamic",
        product=product,
        object_uri=f"s3://raw/{path.name}",
        manifest_uri=f"s3://raw/{path.name}.manifest.json",
        media_type="application/json",
        size_bytes=len(payload),
        checksum=sha256(payload).hexdigest(),
        source_uri="https://example.test/weather",
        valid_time="2026-09-01T00:00:00Z",
        available_at="2026-09-01T01:00:00Z",
        retrieved_at=now,
        first_seen_at=now,
        ingest_run_id="landing-run",
        selection_json=json.dumps(selection),
    )


def _selection(**options: object) -> dict[str, object]:
    return {
        "stream_id": "hourly",
        "window_start": "2026-09-01T00:00:00Z",
        "window_end": "2026-09-01T01:00:00Z",
        "variables": ["precipitation"],
        "source_cycle_id": "20260901T0000Z",
        "source_revision": 7,
        "source_grid_version": "grid-v1",
        "spatial_scope_id": "sonla-scope-v1",
        "cell_count": 1,
        "options": options,
    }


def test_openmeteo_json_expands_points_times_and_variables(tmp_path: Path) -> None:
    path = tmp_path / "ifs.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source_cycle_id": "20260901T0000Z",
                "model_run_time": "2026-09-01T00:00:00Z",
                "responses": [
                    {
                        "latitude": 21.5,
                        "longitude": 104.0,
                        "cell_index": 121,
                        "hourly": {
                            "time": ["2026-09-01T01:00", "2026-09-01T02:00"],
                            "precipitation": [1.0, 2.0],
                            "soil_moisture_0_to_7cm": [0.2, 0.3],
                        },
                        "hourly_units": {
                            "precipitation": "mm",
                            "soil_moisture_0_to_7cm": "m³/m³",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    row = _row(
        path,
        source_id="ifs_openmeteo",
        product="ecmwf_ifs",
        selection={**_selection(), "variables": ["precipitation", "soil_moisture_0_to_7cm"]},
    )

    parsed = list(parse_weather_object(row, path, run_id="bronze-run", parser_version="v1"))

    assert len(parsed) == 4
    assert all(item["cell_indices"] == [121] for item in parsed)
    assert {tuple(item["values"]) for item in parsed} == {(1.0,), (2.0,), (0.2,), (0.3,)}
    assert {item["vertical_level"] for item in parsed} == {"surface", "0_to_7cm"}
    assert {item["value_kind"] for item in parsed} == {"preceding_hour_sum", "instantaneous"}
    assert all(item["valid_time"].tzinfo is UTC for item in parsed)
    precipitation = [item for item in parsed if item["variable"] == "precipitation"]
    assert all(
        item["window_start"].hour + 1 == item["window_end"].hour
        and item["window_end"] == item["valid_time"]
        for item in precipitation
    )


def test_era5_netcdf_expands_grid_and_preserves_source_units(
    tmp_path: Path, monkeypatch
) -> None:
    path = tmp_path / "era5.nc"
    dataset = xr.Dataset(
        {
            "total_precipitation": (
                ("valid_time", "cell_index"),
                np.array([[0.001, 0.002], [0.003, 0.004]], dtype=np.float32),
                {"units": "m"},
            ),
            "volumetric_soil_water_layer_1": (
                ("valid_time", "cell_index"),
                np.array([[0.25, 0.30], [0.26, 0.31]], dtype=np.float32),
                {"units": "m3 m-3"},
            ),
        },
        coords={
            "valid_time": [
                np.datetime64("2026-09-01T00:00:00"),
                np.datetime64("2026-09-01T12:00:00"),
            ],
            "cell_index": [121, 122],
        },
    )
    path.write_bytes(b"netcdf-fixture")
    monkeypatch.setattr(
        "flashflood_data.orchestration.weather.parsers.xr.open_dataset",
        lambda _path: dataset,
    )
    row = _row(
        path,
        source_id="era5_land",
        product="reanalysis-era5-land",
        selection={
            **_selection(),
            "variables": ["total_precipitation", "volumetric_soil_water_layer_1"],
            "cell_count": 2,
        },
    )

    parsed = list(parse_weather_object(row, path, run_id="bronze-run", parser_version="v1"))

    assert len(parsed) == 4
    precipitation = [item for item in parsed if item["variable"] == "total_precipitation"]
    assert {item["unit"] for item in precipitation} == {"m"}
    assert {item["value_kind"] for item in precipitation} == {"accumulation_since_00_utc"}
    assert all(item["window_end"] == item["valid_time"] for item in precipitation)
    midnight = [item for item in precipitation if item["valid_time"].hour == 0]
    noon = [item for item in precipitation if item["valid_time"].hour == 12]
    assert all(item["window_start"] == item["valid_time"] - timedelta(hours=24) for item in midnight)
    assert all(item["window_start"].hour == 0 for item in noon)
    assert all(item["cell_indices"] == [121, 122] for item in parsed)
    assert precipitation[0]["values"] == [0.0010000000474974513, 0.0020000000949949026]


def test_gsmap_npz_emits_one_aligned_raster_slice(tmp_path: Path) -> None:
    path = tmp_path / "gsmap.scoped.npz"
    np.savez(
        path,
        cell_indices=np.asarray([121, 122, 481], dtype="int64"),
        values=np.asarray([1.0, np.nan, 3.5], dtype="float32"),
    )
    selection = {**_selection(), "cell_count": 3}
    row = _row(path, source_id="gsmap", product="gauge_standard_v8", selection=selection)

    parsed = list(parse_weather_object(row, path, run_id="bronze-run", parser_version="v1"))

    assert len(parsed) == 1
    assert {item["unit"] for item in parsed} == {"mm/h"}
    assert {item["value_kind"] for item in parsed} == {"rate"}
    assert parsed[0]["cell_indices"] == [121, 122, 481]
    assert parsed[0]["values"] == [1.0, None, 3.5]


def test_parser_rejects_duplicate_or_unsorted_cell_indices(tmp_path: Path) -> None:
    import pytest

    path = tmp_path / "gsmap.scoped.npz"
    np.savez(
        path,
        cell_indices=np.asarray([122, 121, 121], dtype="int64"),
        values=np.asarray([1.0, 2.0, 3.0], dtype="float32"),
    )
    row = _row(
        path,
        source_id="gsmap",
        product="gauge_standard_v8",
        selection={**_selection(), "cell_count": 3},
    )

    with pytest.raises(ValueError, match="sorted and unique"):
        list(parse_weather_object(row, path, run_id="run", parser_version="v2"))
