"""Physical Iceberg schemas for versioned Silver weather outputs."""

from pathlib import Path

from flashflood_data.shared.metadata.registry import load_dataset_registry
from flashflood_data.storage.iceberg_schemas import table_schema


def test_weight_and_basin_value_schemas_match_domain_contract() -> None:
    weight = table_schema(("silver", "grid_basin_weight"))
    value = table_schema(("silver", "basin_weather_value"))

    assert weight.names == [
        "source_id", "source_grid_version", "source_grid_id", "basin_id", "basin_version",
        "geometry_processing_version", "intersection_area_m2", "weight_by_basin",
        "weight_by_grid", "quality_status",
    ]
    assert value.names == [
        "business_key_hash", "basin_id", "basin_version", "source_id", "source_product",
        "source_grid_version", "source_slice_id", "source_cycle_id", "model_run_time",
        "valid_time", "window_start", "window_end", "variable", "vertical_level",
        "value", "unit", "value_kind", "source_revision", "available_at",
        "valid_coverage_fraction", "quality_flags_json",
    ]
    assert weight.field("intersection_area_m2").nullable is False
    assert value.field("value").nullable is True


def test_meta_registry_declares_both_silver_weather_tables() -> None:
    root = Path(__file__).resolve().parents[3]
    rows = load_dataset_registry(root / "config/shared/metadata.yaml", catalog_name="lake")
    weather = {row["dataset_id"]: row for row in rows if "weather" in row["dataset_id"] or "grid_basin_weight" in row["dataset_id"]}
    assert set(weather) == {"lake.silver.grid_basin_weight", "lake.silver.basin_weather_value"}
    assert {row["quality_policy_id"] for row in weather.values()} == {"silver-weather-v1"}
