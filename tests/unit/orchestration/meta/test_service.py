"""Meta records use stable keys and reject ambiguous lineage/quality evidence."""

from datetime import UTC, datetime

import pytest

from flashflood_data.orchestration.meta.service import MetaRecorder

NOW = datetime(2026, 9, 20, tzinfo=UTC)


class _Store:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, tuple[object, ...]], dict[str, object]] = {}

    def upsert_meta_row(self, identifier, key_fields, row):
        key = (identifier[1], tuple(row[name] for name in key_fields))
        self.rows[key] = dict(row)
        return len(self.rows)

    def upsert_meta_rows(self, identifier, key_fields, rows):
        for row in rows:
            self.upsert_meta_row(identifier, key_fields, row)
        return len(self.rows)

    def get_meta_row(self, identifier, key):
        return self.rows.get((identifier[1], tuple(key.values())))


def test_quality_requires_snapshot_table_and_id_together() -> None:
    recorder = MetaRecorder(_Store())
    with pytest.raises(ValueError, match="snapshot"):
        recorder.record_quality(
            pipeline_run_id="parse-1", dataset_id="bronze.basin_polygon_raw",
            rule_id="unique", status="passed", checked_at=NOW,
            snapshot_table="bronze.basin_polygon_raw", snapshot_id=None,
        )


def test_lineage_has_one_input_kind_and_stable_identity() -> None:
    store = _Store()
    recorder = MetaRecorder(store)
    first = recorder.record_lineage(
        pipeline_run_id="parse-1", input_object_id="raw-1",
        output_table="bronze.basin_polygon_raw", output_snapshot_id=11,
        transform_role="source", mapping_version="v1", created_at=NOW,
    )
    second = recorder.record_lineage(
        pipeline_run_id="parse-1", input_object_id="raw-1",
        output_table="bronze.basin_polygon_raw", output_snapshot_id=11,
        transform_role="source", mapping_version="v1", created_at=NOW,
    )
    assert first == second
    assert len(store.rows) == 1
    with pytest.raises(ValueError, match="exactly one"):
        recorder.record_lineage(
            pipeline_run_id="parse-1", input_object_id="raw-1",
            input_table="meta.source_objects", input_snapshot_id=5,
            output_table="bronze.basin_polygon_raw", output_snapshot_id=11,
            transform_role="source", mapping_version="v1", created_at=NOW,
        )


def test_registry_seed_conflict_fails_closed() -> None:
    recorder = MetaRecorder(_Store())
    source = {
        "source_id": "hydrobasins_v1c", "source_version": "1c",
        "provider": "HydroSHEDS", "dataset": "HydroBASINS", "license_uri": None,
        "coverage_ref": None, "refresh_sla_minutes": None, "valid_from": NOW, "valid_to": None,
    }
    recorder.register_source(source)
    recorder.register_source(source)
    with pytest.raises(ValueError, match="conflicting"):
        recorder.register_source({**source, "provider": "someone else"})


def test_attempts_keep_failed_fetch_and_retry_number() -> None:
    store = _Store()
    recorder = MetaRecorder(store)
    recorder.record_attempt(
        ingest_run_id="landing-1", source_id="cop_dem_glo30_2024_1", asset_id="tile-1",
        attempt_no=2, request_fingerprint="sha256:abc", status="failed",
        started_at=NOW, ended_at=NOW, error_code="http_timeout",
    )
    row = store.rows[("ingest_attempts", ("landing-1", "cop_dem_glo30_2024_1", "tile-1", 2))]
    assert row["status"] == "failed"
    assert row["error_code"] == "http_timeout"
