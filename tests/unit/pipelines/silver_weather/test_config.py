"""Validation of the Silver weather aggregation policy."""

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from flashflood_data.pipelines.silver_weather.config import load_weather_aggregation_config

CONFIG = (
    Path(__file__).resolve().parents[4]
    / "config/pipelines/silver/weather_aggregation.yaml"
)


def _document() -> dict[str, object]:
    return {
        "pipeline_id": "silver_weather_aggregation",
        "contract_version": "1.0",
        "weather_table": "bronze.weather_raster_slice",
        "grid_table": "silver.source_grid",
        "basin_table": "silver.dim_basin",
        "weight_table": "silver.grid_basin_weight",
        "output_table": "silver.basin_weather_value",
        "equal_area_crs": "EPSG:6933",
        "geometry_processing_version": "equal-area-overlay-v1",
        "quality_policy_id": "silver-weather-v1",
        "min_valid_coverage_fraction": 0.8,
        "variables": {
            "precipitation": {
                "unit": "mm",
                "aggregation": "area_mean",
                "value_kind": "interval_sum",
            },
            "runoff": {
                "unit": "mm",
                "aggregation": "area_mean",
                "value_kind": "interval_sum",
            },
            "surface_runoff": {
                "unit": "mm",
                "aggregation": "area_mean",
                "value_kind": "interval_sum",
            },
            "subsurface_runoff": {
                "unit": "mm",
                "aggregation": "area_mean",
                "value_kind": "interval_sum",
            },
            "soil_moisture": {
                "unit": "m3/m3",
                "aggregation": "area_mean",
                "value_kind": "instantaneous",
            },
        },
    }


def _load(tmp_path: Path, document: object):
    path = tmp_path / "weather_aggregation.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return load_weather_aggregation_config(path)


def test_loads_explicit_target_units_and_input_tables(tmp_path: Path) -> None:
    config = _load(tmp_path, _document())
    assert config.weather_table == "bronze.weather_raster_slice"
    assert config.variables["precipitation"].unit == "mm"
    assert config.variables["soil_moisture"].value_kind == "instantaneous"
    assert config.min_valid_coverage_fraction == 0.8


def test_repository_weather_policy_covers_all_target_variables() -> None:
    config = load_weather_aggregation_config(CONFIG)
    assert config.weather_table == "bronze.weather_raster_slice"
    assert config.grid_table == "silver.source_grid"
    assert config.basin_table == "silver.dim_basin"
    assert config.weight_table == "silver.grid_basin_weight"
    assert config.output_table == "silver.basin_weather_value"
    assert config.equal_area_crs == "EPSG:6933"
    assert set(config.variables) >= {
        "precipitation",
        "runoff",
        "surface_runoff",
        "subsurface_runoff",
        "soil_moisture",
    }


def test_rejects_geographic_crs_and_unsafe_coverage(tmp_path: Path) -> None:
    document = _document()
    document["equal_area_crs"] = "EPSG:4326"
    with pytest.raises(ValidationError, match="equal_area_crs"):
        _load(tmp_path, document)

    document["equal_area_crs"] = "EPSG:6933"
    document["min_valid_coverage_fraction"] = 1.2
    with pytest.raises(ValidationError, match="min_valid_coverage_fraction"):
        _load(tmp_path, document)


def test_rejects_missing_or_inconsistent_variable_policy(tmp_path: Path) -> None:
    document = _document()
    document["variables"].pop("subsurface_runoff")
    with pytest.raises(ValidationError, match="subsurface_runoff"):
        _load(tmp_path, document)

    document = _document()
    document["variables"]["precipitation"]["unit"] = "mm/h"
    with pytest.raises(ValidationError, match="precipitation"):
        _load(tmp_path, document)

    document = _document()
    document["variables"]["soil_moisture"]["aggregation"] = "sum"
    with pytest.raises(ValidationError, match="aggregation"):
        _load(tmp_path, document)


def test_rejects_non_mapping_document(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="mapping"):
        _load(tmp_path, ["not", "a", "mapping"])
