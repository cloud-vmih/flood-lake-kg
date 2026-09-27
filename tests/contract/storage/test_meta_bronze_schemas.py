"""Physical contracts that must stay compatible with the lakehouse data dictionary."""

import pyarrow as pa

from flashflood_data.storage.iceberg import source_objects_arrow_schema
from flashflood_data.storage.iceberg_schemas import BRONZE_KEYS, table_schema

META_TABLES = (
    "source_registry", "source_objects", "ingest_attempts", "pipeline_runs",
    "table_snapshot_ref", "parameter_sets", "dataset_registry", "quality_results",
    "lineage_edges", "ingest_watermarks", "object_lifecycle",
)
STATIC_BRONZE_TABLES = (
    "basin_polygon_raw", "river_reach_raw", "osm_feature_raw",
    "raster_coverage", "historical_event_raw", "admin_boundary_raw",
)


def test_meta_contract_has_every_target_table_and_preserves_source_objects() -> None:
    assert all(table_schema(("meta", name)) is not None for name in META_TABLES)
    assert table_schema(("meta", "source_objects")) == source_objects_arrow_schema()
    assert table_schema(("meta", "pipeline_runs")).field("retry_count").type == pa.int32()
    assert table_schema(("meta", "lineage_edges")).field("output_snapshot_id").type == pa.int64()
    watermark = table_schema(("meta", "ingest_watermarks"))
    assert watermark.field("cursor_time").type == pa.timestamp("us", tz="UTC")
    assert watermark.field("spatial_scope_id").nullable is False
    assert watermark.field("detail_json").nullable is False
    lifecycle = table_schema(("meta", "object_lifecycle"))
    assert lifecycle.field("object_id").nullable is False
    assert lifecycle.field("bronze_snapshot_id").type == pa.int64()


def test_static_bronze_contract_has_raw_geometry_and_raster_fields() -> None:
    assert all(table_schema(("bronze", name)) is not None for name in STATIC_BRONZE_TABLES)
    basin = table_schema(("bronze", "basin_polygon_raw"))
    assert basin.field("source_fields_json").nullable is False
    assert basin.field("geometry_wkb").type == pa.binary()
    assert basin.field("bbox_wgs84").type == pa.list_(pa.float64())
    admin = table_schema(("bronze", "admin_boundary_raw"))
    assert admin.field("source_feature_id").nullable is False
    raster = table_schema(("bronze", "raster_coverage"))
    assert raster.field("resolution_x").type == pa.float64()
    assert raster.field("object_uri").nullable is False


def test_weather_slice_uses_parallel_arrays_and_full_business_key() -> None:
    schema = table_schema(("bronze", "weather_raster_slice"))

    assert schema.field("cell_indices").type == pa.list_(pa.int64())
    assert schema.field("values").type == pa.list_(pa.float32())
    assert "weather_grid_value" not in BRONZE_KEYS
    assert BRONZE_KEYS["weather_raster_slice"] == (
        "source_grid_version",
        "spatial_scope_id",
        "variable",
        "vertical_level",
        "source_cycle_id",
        "valid_time",
        "window_start",
        "window_end",
        "source_revision",
    )


def test_source_grid_preserves_stable_cell_identity_and_scope_membership() -> None:
    schema = table_schema(("silver", "source_grid"))

    assert schema.field("cell_index").type == pa.int64()
    assert schema.field("scope_ids").type == pa.list_(pa.string())
    assert schema.field("geometry_wkb").nullable is False


def test_unknown_table_is_rejected() -> None:
    try:
        table_schema(("bronze", "invented"))
    except KeyError:
        return
    raise AssertionError("an unknown table must not receive a guessed schema")
