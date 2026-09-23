from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from flashflood_data.orchestration.weather.config import load_weather_config
from flashflood_data.orchestration.weather.models import WeatherWindow

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("filename", "source_id", "provider"),
    [
        ("gsmap.yaml", "gsmap", "gsmap"),
        ("era5_land.yaml", "era5_land", "era5_land"),
        ("ifs.yaml", "ifs_openmeteo", "ifs_openmeteo"),
    ],
)
def test_dynamic_source_configs_are_operational_and_credential_free(
    filename: str, source_id: str, provider: str
) -> None:
    path = ROOT / "config" / "dynamic" / filename
    config = load_weather_config(path)

    assert config.source_id == source_id
    assert config.provider == provider
    assert config.schedule
    assert config.aoi_path.as_posix().endswith("hydrological_aoi.geoparquet")
    assert config.streams
    assert all(stream.start_at.tzinfo is UTC for stream in config.streams)
    assert all(stream.variables for stream in config.streams)
    text = path.read_text(encoding="utf-8").lower()
    assert "password:" not in text
    assert "api_key:" not in text


def test_configs_cover_the_approved_dynamic_variables() -> None:
    gsmap = load_weather_config(ROOT / "config/dynamic/gsmap.yaml")
    era5 = load_weather_config(ROOT / "config/dynamic/era5_land.yaml")
    ifs = load_weather_config(ROOT / "config/dynamic/ifs.yaml")

    assert {stream.product for stream in gsmap.streams} == {
        "gauge_standard_v8",
        "gauge_now_v8",
    }
    standard, now = gsmap.streams
    assert (standard.step_minutes, standard.chunk_minutes) == (60, 60)
    assert (now.step_minutes, now.chunk_minutes) == (30, 60)
    assert all(stream.options["dtype"] == "<f4" for stream in gsmap.streams)
    assert all(
        tuple(stream.options["missing_values"]) == (-4.0, -8.0, -99.0)
        for stream in gsmap.streams
    )
    assert {variable for stream in gsmap.streams for variable in stream.variables} == {
        "precipitation"
    }
    assert set(era5.streams[0].variables) == {
        "total_precipitation",
        "volumetric_soil_water_layer_1",
        "volumetric_soil_water_layer_2",
        "volumetric_soil_water_layer_3",
        "volumetric_soil_water_layer_4",
        "surface_runoff",
    }
    assert set(ifs.streams[0].variables) == {
        "precipitation",
        "soil_moisture_0_to_7cm",
        "soil_moisture_7_to_28cm",
        "soil_moisture_28_to_100cm",
        "soil_moisture_100_to_255cm",
        "runoff",
    }
    assert ifs.streams[0].options["model"] == "ecmwf_ifs"


def test_weather_window_rejects_naive_or_empty_ranges() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        WeatherWindow(
            start=datetime(2026, 1, 1),  # noqa: DTZ001 - intentionally invalid input
            end=datetime(2026, 1, 1) + timedelta(hours=1),  # noqa: DTZ001
        )
    with pytest.raises(ValueError, match="after start"):
        WeatherWindow(
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 1, 1, tzinfo=UTC),
        )


def test_nested_credentials_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "weather.yaml"
    path.write_text(
        "source_id: unsafe\nstreams:\n  - options:\n      api_key: committed-secret\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="credentials"):
        load_weather_config(path)
