"""Object-scoped writes and Meta upserts must preserve unrelated records."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo, In, Or
from pyiceberg.expressions.visitors import bind
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from flashflood_data.storage.iceberg_tables import IcebergTableStore, _filters


def _matches(row: dict[str, object], expression: object) -> bool:
    if isinstance(expression, EqualTo):
        return row[expression.term.name] == expression.literal.value
    if isinstance(expression, And):
        return _matches(row, expression.left) and _matches(row, expression.right)
    if isinstance(expression, Or):
        return _matches(row, expression.left) or _matches(row, expression.right)
    if isinstance(expression, In):
        return row[expression.term.name] in {literal.value for literal in expression.literals}
    raise AssertionError(f"unexpected filter: {expression!r}")


class _Scan:
    def __init__(self, table: "_Table", expression: object) -> None:
        self.table = table
        self.expression = expression

    def to_arrow(self) -> pa.Table:
        rows = [row for row in self.table.rows if _matches(row, self.expression)]
        return pa.Table.from_pylist(rows, schema=self.table.schema)


class _Table:
    def __init__(self, schema: pa.Schema) -> None:
        self.schema = schema
        self.rows: list[dict[str, object]] = []
        self.snapshot_id: int | None = None
        self.overwrites = 0
        self.appends = 0
        self.fail_next_overwrites = 0
        self.fail_next_appends = 0
        self.scan_filters: list[object] = []

    def refresh(self) -> None:
        pass

    def scan(self, *, row_filter: object) -> _Scan:
        self.scan_filters.append(row_filter)
        return _Scan(self, row_filter)

    def overwrite(self, data: pa.Table, *, overwrite_filter: object) -> None:
        if self.fail_next_overwrites:
            self.fail_next_overwrites -= 1
            raise CommitFailedException("concurrent snapshot")
        self.rows = [row for row in self.rows if not _matches(row, overwrite_filter)]
        self.rows.extend(data.to_pylist())
        self.overwrites += 1
        self.snapshot_id = (self.snapshot_id or 0) + 1

    def append(self, data: pa.Table) -> None:
        if self.fail_next_appends:
            self.fail_next_appends -= 1
            raise CommitFailedException("concurrent snapshot")
        self.rows.extend(data.to_pylist())
        self.appends += 1
        self.snapshot_id = (self.snapshot_id or 0) + 1

    def delete(self, expression: object) -> None:
        self.rows = [row for row in self.rows if not _matches(row, expression)]
        self.snapshot_id = (self.snapshot_id or 0) + 1

    def current_snapshot(self) -> SimpleNamespace | None:
        return None if self.snapshot_id is None else SimpleNamespace(snapshot_id=self.snapshot_id)

    def transaction(self):
        return _Transaction(self)


class _Transaction:
    def __init__(self, table: _Table) -> None:
        self.table = table
        self.rows = list(table.rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.table.rows = self.rows
            self.table.snapshot_id = (self.table.snapshot_id or 0) + 1

    def delete(self, expression):
        self.rows = [row for row in self.rows if not _matches(row, expression)]

    def append(self, data: pa.Table):
        self.rows.extend(data.to_pylist())


def test_large_key_filter_can_be_bound_by_pyiceberg() -> None:
    schema = Schema(
        NestedField(1, "source_id", StringType(), required=True),
        NestedField(2, "source_grid_version", StringType(), required=True),
        NestedField(3, "source_grid_id", StringType(), required=True),
    )
    keys = [
        {
            "source_id": "gsmap",
            "source_grid_version": "grid-v1",
            "source_grid_id": f"row=1000,col={column}",
        }
        for column in range(500)
    ]

    expression = _filters(keys)

    assert bind(schema, expression, case_sensitive=True) is not None


class _Catalog:
    def __init__(self) -> None:
        self.tables: dict[tuple[str, str], _Table] = {}

    def create_namespace_if_not_exists(self, namespace: tuple[str]) -> None:
        assert namespace[0] in {"meta", "bronze", "silver", "quarantine"}

    def create_table_if_not_exists(
        self, identifier: tuple[str, str], *, schema: pa.Schema, properties: dict[str, str]
    ) -> _Table:
        assert properties["format-version"] == "2"
        return self.tables.setdefault(identifier, _Table(schema))


def _basin(object_id: str, version: str = "v1", feature_id: str = "42") -> dict[str, object]:
    return {
        "object_id": object_id,
        "source_feature_id": feature_id,
        "source_id": "hydrobasins_v1c",
        "source_fields_json": '{"HYBAS_ID":42}',
        "geometry_wkb": b"geometry",
        "crs": "EPSG:4326",
        "bbox_wgs84": [103.0, 21.0, 104.0, 22.0],
        "ingest_run_id": "run-a",
        "parser_version": version,
        "quality_status": "passed",
    }


def test_retry_skips_equivalent_object_and_reparse_replaces_only_its_rows() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    first = store.replace_object_rows(("bronze", "basin_polygon_raw"), "a", [_basin("a")])
    other = store.replace_object_rows(("bronze", "basin_polygon_raw"), "b", [_basin("b")])
    retried = {**_basin("a"), "ingest_run_id": "run-b"}
    assert store.replace_object_rows(("bronze", "basin_polygon_raw"), "a", [retried]) == other
    table = catalog.tables[("bronze", "basin_polygon_raw")]
    assert table.overwrites == 2
    changed = {**_basin("a", version="v2"), "source_fields_json": '{"HYBAS_ID":42,"NEXT_DOWN":7}'}
    assert store.replace_object_rows(("bronze", "basin_polygon_raw"), "a", [changed]) > first
    assert len(table.rows) == 2
    assert {row["object_id"]: row["parser_version"] for row in table.rows} == {"a": "v2", "b": "v1"}


def test_object_write_rejects_duplicate_feature_keys_and_wrong_object() -> None:
    store = IcebergTableStore(_Catalog())
    with pytest.raises(ValueError, match="duplicate"):
        store.replace_object_rows(("bronze", "basin_polygon_raw"), "a", [_basin("a"), _basin("a")])
    with pytest.raises(ValueError, match="object_id"):
        store.replace_object_rows(("bronze", "basin_polygon_raw"), "a", [_basin("b")])


def test_meta_upsert_replaces_one_run_without_touching_another() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    base = {
        "pipeline_run_id": "run-a",
        "orchestrator_run_id": None,
        "job_name": "bronze_parse",
        "code_git_sha": None,
        "image_digest": None,
        "config_hash": "config-1",
        "parameter_set_id": None,
        "started_at": datetime(2026, 9, 20, tzinfo=UTC),
        "finished_at": None,
        "published_at": None,
        "status": "running",
        "retry_count": 0,
        "input_row_count": None,
        "output_row_count": None,
        "quality_result_json": None,
        "metrics_json": None,
        "error_code": None,
    }
    store.upsert_meta_row(("meta", "pipeline_runs"), ("pipeline_run_id",), base)
    store.upsert_meta_row(
        ("meta", "pipeline_runs"), ("pipeline_run_id",), {**base, "pipeline_run_id": "run-b"}
    )
    store.upsert_meta_row(
        ("meta", "pipeline_runs"), ("pipeline_run_id",), {**base, "status": "succeeded"}
    )
    rows = catalog.tables[("meta", "pipeline_runs")].rows
    assert {row["pipeline_run_id"]: row["status"] for row in rows} == {
        "run-a": "succeeded",
        "run-b": "running",
    }
    assert (
        store.get_meta_row(("meta", "pipeline_runs"), {"pipeline_run_id": "run-a"})["status"]
        == "succeeded"
    )


def test_meta_batch_upsert_commits_many_keys_once_and_preserves_other_rows() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "ingest_attempts")

    def attempt(asset_id: str) -> dict[str, object]:
        return {
            "ingest_run_id": "landing-1",
            "source_id": "soilgrids_2_0",
            "asset_id": asset_id,
            "attempt_no": 1,
            "request_fingerprint": f"fingerprint-{asset_id}",
            "http_status": None,
            "error_code": None,
            "started_at": datetime(2026, 9, 20, tzinfo=UTC),
            "ended_at": datetime(2026, 9, 20, tzinfo=UTC),
            "status": "succeeded",
        }

    keys = ("ingest_run_id", "source_id", "asset_id", "attempt_no")
    store.upsert_meta_row(identifier, keys, attempt("unrelated"))
    table = catalog.tables[identifier]
    before = table.appends

    snapshot = store.upsert_meta_rows(identifier, keys, [attempt("tile-1"), attempt("tile-2")])

    assert snapshot == table.snapshot_id
    assert table.appends == before + 1
    assert {row["asset_id"] for row in table.rows} == {
        "unrelated",
        "tile-1",
        "tile-2",
    }


def test_quality_result_batch_uses_bounded_run_filter_and_preserves_retry_rows() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "quality_results")
    keys = (
        "pipeline_run_id",
        "check_phase",
        "dataset_id",
        "rule_id",
        "rule_version",
        "scope_key",
    )

    def result(run_id: str, scope_key: str, phase: str = "post_commit") -> dict[str, object]:
        return {
            "pipeline_run_id": run_id,
            "check_phase": phase,
            "dataset_id": "bronze.weather_raster_slice",
            "rule_id": "source_grid_complete",
            "rule_version": "v1",
            "scope_key": scope_key,
            "severity": "fatal",
            "status": "passed",
            "observed_value_json": None,
            "expected_value_json": None,
            "failed_row_count": None,
            "sample_uri": None,
            "snapshot_table": "bronze.weather_raster_slice",
            "snapshot_id": 7,
            "checked_at": datetime(2026, 10, 24, tzinfo=UTC),
        }

    store.upsert_meta_rows(identifier, keys, [result("other-run", "other")])
    store.upsert_meta_rows(identifier, keys, [result("run-a", "pre", "pre_commit")])
    table = catalog.tables[identifier]
    table.scan_filters.clear()
    rows = [result("run-a", f"slice-{index}") for index in range(300)]

    store.upsert_meta_rows(identifier, keys, rows)

    assert len(table.scan_filters) == 1
    assert isinstance(table.scan_filters[0], EqualTo)
    assert table.scan_filters[0].term.name == "pipeline_run_id"
    assert table.scan_filters[0].literal.value == "run-a"

    changed = {**rows[0], "status": "failed"}
    store.upsert_meta_rows(identifier, keys, [changed])
    store.upsert_meta_rows(identifier, keys, [changed])
    assert len(table.rows) == 302
    assert next(row for row in table.rows if row["scope_key"] == "slice-0")["status"] == "failed"
    assert {row["scope_key"] for row in table.rows} >= {"other", "pre", "slice-299"}


def test_lifecycle_batch_uses_one_in_filter_for_read_and_upsert() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "object_lifecycle")

    def lifecycle_row(object_id: str) -> dict[str, object]:
        return {
            "object_id": object_id,
            "retention_class": "durable",
            "storage_status": "available",
            "expires_at": None,
            "bronze_snapshot_id": None,
            "quality_status": "pending",
            "lineage_edge_id": None,
            "deleted_at": None,
            "last_checked_at": datetime(2026, 10, 27, tzinfo=UTC),
            "reason": None,
        }

    store.upsert_meta_rows(identifier, ("object_id",), [lifecycle_row("a"), lifecycle_row("b")])
    table = catalog.tables[identifier]
    assert isinstance(table.scan_filters[-1], In)
    table.scan_filters.clear()

    rows = store.get_meta_rows(identifier, "object_id", ["a", "missing", "b"])

    assert {row["object_id"] for row in rows} == {"a", "b"}
    assert len(table.scan_filters) == 1
    assert isinstance(table.scan_filters[0], In)


def test_ingest_attempt_batch_filters_assets_without_rewriting_other_attempts() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "ingest_attempts")
    keys = ("ingest_run_id", "source_id", "asset_id", "attempt_no")

    def attempt(run_id: str, asset_id: str, number: int = 1) -> dict[str, object]:
        return {
            "ingest_run_id": run_id,
            "source_id": "gsmap",
            "asset_id": asset_id,
            "attempt_no": number,
            "request_fingerprint": f"fingerprint-{asset_id}",
            "http_status": None,
            "error_code": None,
            "started_at": datetime(2026, 10, 27, tzinfo=UTC),
            "ended_at": datetime(2026, 10, 27, tzinfo=UTC),
            "status": "succeeded",
        }

    store.upsert_meta_rows(identifier, keys, [attempt("other-run", "hour-0")])
    store.upsert_meta_rows(identifier, keys, [attempt("run-a", "hour-0", 2)])
    table = catalog.tables[identifier]
    table.scan_filters.clear()
    batch = [attempt("run-a", f"hour-{index}") for index in range(24)]

    store.upsert_meta_rows(identifier, keys, batch)

    expression = table.scan_filters[-1]
    assert isinstance(expression, And)
    assert "In(" in str(expression)
    assert _matches(batch[0], expression)
    assert not _matches(attempt("other-run", "hour-0"), expression)
    assert not _matches(attempt("run-a", "hour-0", 2), expression)

    store.upsert_meta_rows(identifier, keys, [{**batch[0], "status": "failed"}])
    assert len(table.rows) == 26
    assert next(
        row for row in table.rows
        if row["ingest_run_id"] == "run-a" and row["asset_id"] == "hour-0"
        and row["attempt_no"] == 1
    )["status"] == "failed"
    assert any(row["ingest_run_id"] == "other-run" for row in table.rows)
    assert any(row["ingest_run_id"] == "run-a" and row["attempt_no"] == 2 for row in table.rows)


def test_repeated_successful_ingest_attempt_keeps_first_audit_without_overwrite() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "ingest_attempts")
    keys = ("ingest_run_id", "source_id", "asset_id", "attempt_no")
    first = {
        "ingest_run_id": "run-a",
        "source_id": "gsmap",
        "asset_id": "hour-0",
        "attempt_no": 1,
        "request_fingerprint": "same-request",
        "http_status": 200,
        "error_code": None,
        "started_at": datetime(2026, 10, 27, 10, tzinfo=UTC),
        "ended_at": datetime(2026, 10, 27, 10, 1, tzinfo=UTC),
        "status": "succeeded",
    }
    snapshot = store.upsert_meta_rows(identifier, keys, [first])
    table = catalog.tables[identifier]

    repeated = {
        **first,
        "http_status": None,
        "started_at": datetime(2026, 10, 27, 11, tzinfo=UTC),
        "ended_at": datetime(2026, 10, 27, 11, 1, tzinfo=UTC),
    }
    assert store.upsert_meta_rows(identifier, keys, [repeated]) == snapshot
    assert table.overwrites == 0
    assert table.rows == [first]

    failed = {**repeated, "status": "failed", "error_code": "PublishError"}
    store.upsert_meta_rows(identifier, keys, [failed])
    assert table.overwrites == 1
    assert table.rows[0]["status"] == "failed"


def test_meta_new_keys_use_one_append_without_planning_an_overwrite() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "pipeline_runs")
    rows = [
        {
            "pipeline_run_id": run_id,
            "orchestrator_run_id": None,
            "job_name": "raw_landing",
            "code_git_sha": None,
            "image_digest": None,
            "config_hash": "config-1",
            "parameter_set_id": None,
            "started_at": datetime(2026, 9, 20, tzinfo=UTC),
            "finished_at": None,
            "published_at": None,
            "status": "running",
            "retry_count": 0,
            "input_row_count": None,
            "output_row_count": None,
            "quality_result_json": None,
            "metrics_json": None,
            "error_code": None,
        }
        for run_id in ("run-a", "run-b")
    ]

    store.upsert_meta_rows(identifier, ("pipeline_run_id",), rows)

    table = catalog.tables[identifier]
    assert table.appends == 1
    assert table.overwrites == 0


def test_keyed_silver_upsert_merges_grid_scope_without_duplicate_cells() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("silver", "source_grid")
    base = {
        "source_id": "gsmap",
        "source_grid_version": "v1",
        "source_grid_id": "row=1110,col=2830",
        "cell_index": 11102830,
        "row_index": 1110,
        "column_index": 2830,
        "geometry_wkb": b"polygon",
        "bbox_wgs84": [103.0, 21.0, 103.1, 21.1],
        "centroid_lon": 103.05,
        "centroid_lat": 21.05,
        "resolution_x": 0.1,
        "resolution_y": 0.1,
        "crs": "EPSG:4326",
        "scope_ids": ["vietnam-l12-h1"],
    }
    keys = ("source_id", "source_grid_version", "source_grid_id")

    store.upsert_keyed_rows(identifier, keys, [base])
    store.upsert_keyed_rows(
        identifier, keys, [{**base, "scope_ids": ["sonla-l12-h1", "vietnam-l12-h1"]}]
    )

    table = catalog.tables[identifier]
    assert len(table.rows) == 1
    assert table.rows[0]["scope_ids"] == ["sonla-l12-h1", "vietnam-l12-h1"]


def test_meta_upsert_refreshes_and_retries_optimistic_commit_conflict() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "pipeline_runs")
    table = store.ensure_table(identifier)
    table.fail_next_appends = 1
    row = {
        "pipeline_run_id": "run-a",
        "orchestrator_run_id": None,
        "job_name": "bronze_parse",
        "code_git_sha": None,
        "image_digest": None,
        "config_hash": "config-1",
        "parameter_set_id": None,
        "started_at": datetime(2026, 9, 20, tzinfo=UTC),
        "finished_at": None,
        "published_at": None,
        "status": "running",
        "retry_count": 0,
        "input_row_count": None,
        "output_row_count": None,
        "quality_result_json": None,
        "metrics_json": None,
        "error_code": None,
    }

    snapshot = store.upsert_meta_row(identifier, ("pipeline_run_id",), row)

    assert snapshot == 1
    assert table.rows == [row]


def test_concurrent_conflicting_writes_produce_distinct_snapshots() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    first_rows = [_basin("obj-1", version="v1", feature_id="feat-1")]
    second_rows = [_basin("obj-1", version="v1", feature_id="feat-2")]

    snap1 = store.replace_object_rows(("bronze", "basin_polygon_raw"), "obj-1", first_rows)
    snap2 = store.replace_object_rows(("bronze", "basin_polygon_raw"), "obj-1", second_rows)

    assert snap1 != snap2
    table = catalog.tables[("bronze", "basin_polygon_raw")]
    assert len(table.rows) == 1
    assert table.rows[0]["source_feature_id"] == "feat-2"


def test_batched_object_replacement_is_atomic_and_preserves_other_objects() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    table_id = ("bronze", "basin_polygon_raw")
    store.replace_object_rows(table_id, "other", [_basin("other")])

    snapshot, count = store.replace_object_batches(
        table_id, "a", iter([[_basin("a", feature_id="1")], [_basin("a", feature_id="2")]])
    )
    table = catalog.tables[table_id]
    assert count == 2
    assert snapshot == table.snapshot_id
    assert {(row["object_id"], row["source_feature_id"]) for row in table.rows} == {
        ("other", "42"),
        ("a", "1"),
        ("a", "2"),
    }

    def broken_batches():
        yield [_basin("a", feature_id="new")]
        raise ValueError("parser failed in later batch")

    with pytest.raises(ValueError, match="later batch"):
        store.replace_object_batches(table_id, "a", broken_batches())
    assert {(row["object_id"], row["source_feature_id"]) for row in table.rows} == {
        ("other", "42"),
        ("a", "1"),
        ("a", "2"),
    }


def test_batched_object_replacement_rejects_duplicate_keys_across_batches() -> None:
    store = IcebergTableStore(_Catalog())
    with pytest.raises(ValueError, match="duplicate"):
        store.replace_object_batches(
            ("bronze", "basin_polygon_raw"),
            "a",
            iter([[_basin("a", feature_id="1")], [_basin("a", feature_id="1")]]),
        )


def test_multi_object_replacement_commits_one_snapshot_and_preserves_other_objects() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    table_id = ("bronze", "basin_polygon_raw")
    store.replace_object_rows(table_id, "other", [_basin("other")])
    before = catalog.tables[table_id].snapshot_id

    snapshot = store.replace_objects_rows(
        table_id,
        ("a", "b"),
        [_basin("a", feature_id="1"), _basin("b", feature_id="2")],
    )

    table = catalog.tables[table_id]
    assert snapshot == before + 1
    assert {(row["object_id"], row["source_feature_id"]) for row in table.rows} == {
        ("other", "42"),
        ("a", "1"),
        ("b", "2"),
    }


def test_multi_object_replacement_requires_rows_for_every_requested_object() -> None:
    store = IcebergTableStore(_Catalog())
    with pytest.raises(ValueError, match="every requested object"):
        store.replace_objects_rows(
            ("bronze", "basin_polygon_raw"),
            ("a", "b"),
            [_basin("a", feature_id="1")],
        )


def test_partition_replacement_keeps_valid_objects_and_removes_blocked_slice() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    table_id = ("bronze", "basin_polygon_raw")
    store.replace_object_rows(table_id, "blocked", [_basin("blocked")])
    store.replace_object_rows(table_id, "other", [_basin("other")])

    snapshot = store.replace_object_partitions(
        table_id,
        {"valid": [_basin("valid", feature_id="7")]},
        ("blocked",),
    )

    table = catalog.tables[table_id]
    assert snapshot == table.snapshot_id
    assert {(row["object_id"], row["source_feature_id"]) for row in table.rows} == {
        ("other", "42"),
        ("valid", "7"),
    }


def test_blocked_only_partition_removes_old_slice_and_empty_table_is_noop() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    table_id = ("bronze", "basin_polygon_raw")

    assert store.replace_object_partitions(table_id, {}, ("missing",)) is None
    store.replace_object_rows(table_id, "blocked", [_basin("blocked")])

    snapshot = store.replace_object_partitions(table_id, {}, ("blocked",))

    assert snapshot == catalog.tables[table_id].snapshot_id
    assert catalog.tables[table_id].rows == []


def test_quarantine_upsert_is_keyed_and_rejects_duplicate_payload_keys() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("quarantine", "rejected_rows")
    row = {
        "rejection_id": "reject-1",
        "pipeline_run_id": "run-1",
        "layer": "bronze",
        "target_dataset_id": "bronze.river_reach_raw",
        "object_id": "object-1",
        "input_table": None,
        "input_snapshot_id": None,
        "source_record_key": "feature-1",
        "quality_policy_id": "policy-1",
        "rule_ids": ["geometry_valid"],
        "reason_codes_json": '["invalid_geometry"]',
        "record_json": '{"id":"feature-1"}',
        "record_uri": None,
        "parser_version": "v2",
        "rejected_at": datetime(2026, 10, 11, tzinfo=UTC),
    }

    first = store.upsert_quarantine_rows(identifier, ("rejection_id",), [row])
    second = store.upsert_quarantine_rows(identifier, ("rejection_id",), [row])

    assert first == second
    assert catalog.tables[identifier].rows == [row]
    with pytest.raises(ValueError, match="duplicate requested key"):
        store.upsert_quarantine_rows(identifier, ("rejection_id",), [row, row])
