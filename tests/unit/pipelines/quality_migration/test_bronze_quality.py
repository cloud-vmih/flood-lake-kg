from types import SimpleNamespace

from flashflood_data.pipelines.quality_migration.bronze_quality import (
    BronzeQualityMigration,
)


class Backend:
    def __init__(self, rows, raw=None) -> None:
        self.rows = rows
        self.raw = raw or {}
        self.provider_calls = []
        self.audit = []
        self.rewrites = []
        self.reprocesses = []

    def bronze_rows(self, dataset_id, snapshot_id):
        return list(self.rows)

    def raw_object(self, object_id):
        return self.raw.get(object_id)

    def raw_exists(self, object_uri):
        return object_uri != "s3://raw/expired.bin"

    def checkpoint_exists(self, checkpoint_id):
        return False

    def record_decisions(self, decisions, run_id, mode):
        self.audit.extend(decisions)

    def rewrite_object(self, dataset_id, object_id, rows):
        self.rewrites.append((dataset_id, object_id, list(rows)))

    def reprocess_object(self, dataset_id, object_id, run_id):
        self.reprocesses.append((dataset_id, object_id, run_id))


def _vector_row(object_id="object-1", quality_status="passed"):
    return {
        "object_id": object_id,
        "source_feature_id": "feature-1",
        "source_id": "hydrobasins_v1c",
        "source_fields_json": "{}",
        "geometry_wkb": b"geometry",
        "crs": "EPSG:4326",
        "bbox_wgs84": [103.0, 20.0, 104.0, 21.0],
        "ingest_run_id": "old-run",
        "parser_version": "v1",
        "quality_status": quality_status,
    }


def _raster_row(object_id="raster-1"):
    return {
        "object_id": object_id,
        "band_or_layer": "1",
        "property": "elevation",
        "depth_interval": None,
        "statistic": None,
        "object_uri": "s3://raw/raster.tif",
        "crs": "EPSG:4326",
        "bbox_wgs84": [103.0, 20.0, 104.0, 21.0],
        "resolution_x": 0.1,
        "resolution_y": 0.1,
        "nodata": None,
        "dtype": "float32",
        "checksum": "a" * 64,
        "ingest_run_id": "old-run",
        "parser_version": "v1",
        "quality_status": "passed",
    }


def test_existing_bronze_columns_use_audit_only() -> None:
    backend = Backend([_vector_row()])
    migration = BronzeQualityMigration(backend)

    decisions = migration.plan("bronze.basin_polygon_raw", 42, "bronze-static-v1")

    assert decisions[0].action == "audit_only"
    assert backend.provider_calls == []


def test_pixel_rule_uses_existing_raw_before_refetch() -> None:
    backend = Backend(
        [_raster_row()],
        raw={"raster-1": SimpleNamespace(object_uri="s3://raw/raster.tif")},
    )
    migration = BronzeQualityMigration(backend)

    decisions = migration.plan("bronze.raster_coverage", 42, "bronze-static-v1")

    assert decisions[0].action == "reprocess_raw"
    assert decisions[0].raw_uri == "s3://raw/raster.tif"
    assert backend.provider_calls == []


def test_expired_transient_raw_is_marked_unavailable() -> None:
    backend = Backend(
        [_raster_row()],
        raw={"raster-1": SimpleNamespace(object_uri="s3://raw/expired.bin")},
    )
    migration = BronzeQualityMigration(backend)

    decision = migration.plan("bronze.raster_coverage", 42, "policy-v1")[0]

    assert decision.action == "mark_raw_unavailable"


def test_audit_mode_never_rewrites_or_reprocesses_target() -> None:
    backend = Backend(
        [_raster_row()],
        raw={"raster-1": SimpleNamespace(object_uri="s3://raw/raster.tif")},
    )
    migration = BronzeQualityMigration(backend)
    decisions = migration.plan("bronze.raster_coverage", 42, "policy-v1")

    summary = migration.execute(decisions, "migration-run", mode="audit")

    assert summary.audited == 1
    assert backend.audit == list(decisions)
    assert backend.rewrites == []
    assert backend.reprocesses == []


def test_apply_normalizes_legacy_warning_without_duplicate_object() -> None:
    backend = Backend([_vector_row(quality_status="flagged")])
    migration = BronzeQualityMigration(backend)
    decisions = migration.plan("bronze.basin_polygon_raw", 42, "policy-v1")

    summary = migration.execute(decisions, "migration-run", mode="apply")

    assert decisions[0].action == "rewrite_bronze"
    assert summary.rewritten == 1
    assert backend.rewrites[0][2][0]["quality_status"] == "warning"
