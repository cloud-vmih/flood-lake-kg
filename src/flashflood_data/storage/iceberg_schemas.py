"""Physical Arrow schemas for cross-pipeline Meta and Bronze Iceberg tables."""

import pyarrow as pa

from flashflood_data.storage.iceberg import source_objects_arrow_schema

_TYPES = {
    "string": pa.string(),
    "json": pa.string(),
    "int": pa.int32(),
    "long": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "binary": pa.binary(),
    "bbox": pa.list_(pa.float64()),
    "list_long": pa.list_(pa.int64()),
    "list_float": pa.list_(pa.float32()),
    "list_string": pa.list_(pa.string()),
}

BRONZE_KEYS: dict[str, tuple[str, ...]] = {
    "basin_polygon_raw": ("source_feature_id",),
    "river_reach_raw": ("source_feature_id",),
    "admin_boundary_raw": ("source_feature_id",),
    "osm_feature_raw": ("osm_type", "osm_id"),
    "raster_coverage": ("band_or_layer",),
    "historical_event_raw": ("source_record_id",),
    "weather_raster_slice": (
        "source_grid_version",
        "spatial_scope_id",
        "variable",
        "vertical_level",
        "source_cycle_id",
        "valid_time",
        "window_start",
        "window_end",
        "source_revision",
    ),
}


def _schema(definition: str) -> pa.Schema:
    fields = []
    for line in definition.strip().splitlines():
        name, kind, required = line.split()
        fields.append(pa.field(name, _TYPES[kind], nullable=required != "!"))
    return pa.schema(fields)


_DEFINITIONS = {
    ("meta", "source_registry"): """
        source_id string !
        source_version string !
        provider string !
        dataset string !
        license_uri string ?
        coverage_ref string ?
        refresh_sla_minutes int ?
        valid_from timestamp !
        valid_to timestamp ?
    """,
    ("meta", "ingest_attempts"): """
        ingest_run_id string !
        source_id string !
        asset_id string !
        attempt_no int !
        request_fingerprint string !
        http_status int ?
        error_code string ?
        started_at timestamp !
        ended_at timestamp ?
        status string !
    """,
    ("meta", "pipeline_runs"): """
        pipeline_run_id string !
        orchestrator_run_id string ?
        job_name string !
        code_git_sha string ?
        image_digest string ?
        config_hash string !
        parameter_set_id string ?
        started_at timestamp !
        finished_at timestamp ?
        published_at timestamp ?
        status string !
        retry_count int !
        input_row_count long ?
        output_row_count long ?
        quality_result_json json ?
        metrics_json json ?
        error_code string ?
    """,
    ("meta", "table_snapshot_ref"): """
        pipeline_run_id string !
        table_name string !
        iceberg_snapshot_id long !
        role string !
        created_at timestamp !
        quality_status string !
    """,
    ("meta", "parameter_sets"): """
        parameter_set_id string !
        parameter_type string !
        method_version string !
        parameters_json json !
        beta double ?
        alpha_1h double ?
        valid_from timestamp !
        valid_to timestamp ?
        is_active boolean !
    """,
    ("meta", "dataset_registry"): """
        dataset_id string !
        contract_version string !
        layer string !
        description string !
        owner string !
        source_id string ?
        schema_ref string !
        data_classification string !
        license_id string ?
        retention_policy_ref string !
        freshness_sla_minutes int ?
        quality_policy_id string !
        access_policy_ref string !
        valid_from timestamp !
        valid_to timestamp ?
    """,
    ("meta", "quality_results"): """
        pipeline_run_id string !
        check_phase string !
        dataset_id string !
        rule_id string !
        rule_version string !
        scope_key string !
        severity string !
        status string !
        observed_value_json json ?
        expected_value_json json ?
        failed_row_count long ?
        sample_uri string ?
        snapshot_table string ?
        snapshot_id long ?
        checked_at timestamp !
    """,
    ("meta", "lineage_edges"): """
        lineage_edge_id string !
        pipeline_run_id string !
        input_kind string !
        input_object_id string ?
        input_table string ?
        input_snapshot_id long ?
        output_table string !
        output_snapshot_id long !
        transform_role string !
        mapping_version string ?
        created_at timestamp !
    """,
    ("meta", "ingest_watermarks"): """
        source_id string !
        product string !
        stream_id string !
        spatial_scope_id string !
        cursor_time timestamp !
        last_safe_end timestamp !
        last_run_id string !
        status string !
        updated_at timestamp !
        detail_json json !
    """,
    ("meta", "object_lifecycle"): """
        object_id string !
        retention_class string !
        storage_status string !
        expires_at timestamp ?
        bronze_snapshot_id long ?
        quality_status string !
        lineage_edge_id string ?
        deleted_at timestamp ?
        last_checked_at timestamp !
        reason string ?
    """,
    ("bronze", "basin_polygon_raw"): """
        object_id string !
        source_feature_id string !
        source_id string !
        source_fields_json json !
        geometry_wkb binary !
        crs string !
        bbox_wgs84 bbox !
        ingest_run_id string !
        parser_version string !
        quality_status string !
    """,
    ("bronze", "river_reach_raw"): """
        object_id string !
        source_feature_id string !
        source_fields_json json !
        geometry_wkb binary !
        crs string !
        bbox_wgs84 bbox !
        ingest_run_id string !
        parser_version string !
        quality_status string !
    """,
    ("bronze", "admin_boundary_raw"): """
        object_id string !
        source_feature_id string !
        source_id string !
        source_fields_json json !
        geometry_wkb binary !
        crs string !
        bbox_wgs84 bbox !
        ingest_run_id string !
        parser_version string !
        quality_status string !
    """,
    ("bronze", "osm_feature_raw"): """
        object_id string !
        osm_type string !
        osm_id string !
        tags_json json !
        geometry_wkb binary ?
        crs string ?
        bbox_wgs84 bbox ?
        ingest_run_id string !
        parser_version string !
        quality_status string !
    """,
    ("bronze", "raster_coverage"): """
        object_id string !
        band_or_layer string !
        property string ?
        depth_interval string ?
        statistic string ?
        object_uri string !
        crs string !
        bbox_wgs84 bbox !
        resolution_x double !
        resolution_y double !
        nodata double ?
        dtype string !
        checksum string !
        ingest_run_id string !
        parser_version string !
        quality_status string !
    """,
    ("bronze", "historical_event_raw"): """
        object_id string !
        source_record_id string !
        source_document_id string ?
        event_text string ?
        original_fields_json json !
        source_valid_time string ?
        ingest_run_id string !
        parser_version string !
        quality_status string !
    """,
    ("bronze", "weather_raster_slice"): """
        slice_id string !
        object_id string !
        source_id string !
        source_product string !
        source_grid_version string !
        spatial_scope_id string !
        variable string !
        vertical_level string !
        source_cycle_id string !
        model_run_time timestamp ?
        valid_time timestamp !
        window_start timestamp !
        window_end timestamp !
        source_revision int !
        cell_indices list_long !
        values list_float !
        unit string !
        value_kind string !
        available_at timestamp ?
        ingest_run_id string !
        parser_version string !
        quality_status string !
    """,
    ("silver", "source_grid"): """
        source_id string !
        source_grid_version string !
        source_grid_id string !
        cell_index long !
        row_index int !
        column_index int !
        geometry_wkb binary !
        bbox_wgs84 bbox !
        centroid_lon double !
        centroid_lat double !
        resolution_x double !
        resolution_y double !
        crs string !
        scope_ids list_string !
    """,
}


def table_schema(identifier: tuple[str, str]) -> pa.Schema:
    """Return a frozen physical schema; reject tables outside the contract."""
    if identifier in _DEFINITIONS:
        return _schema(_DEFINITIONS[identifier])
    if identifier == ("meta", "source_objects") or (
        len(identifier) == 2 and identifier[1] == "source_objects" and identifier[0].startswith("smoke_")
    ):
        return source_objects_arrow_schema()
    if len(identifier) == 2 and identifier[0].startswith("smoke_"):
        for (_, name), definition in _DEFINITIONS.items():
            if identifier[1] == name:
                return _schema(definition)
    raise KeyError(f"table identifier outside contract: {identifier}")
