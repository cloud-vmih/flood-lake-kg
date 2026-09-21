"""Object-scoped writes and Meta upserts must preserve unrelated records."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo

from flashflood_data.storage.iceberg_tables import IcebergTableStore


def _matches(row: dict[str, object], expression: object) -> bool:
    if isinstance(expression, EqualTo):
        return row[expression.term.name] == expression.literal.value
    if isinstance(expression, And):
        return _matches(row, expression.left) and _matches(row, expression.right)
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
        self.fail_next_overwrites = 0

    def refresh(self) -> None:
        pass

    def scan(self, *, row_filter: object) -> _Scan:
        return _Scan(self, row_filter)

    def overwrite(self, data: pa.Table, *, overwrite_filter: object) -> None:
        if self.fail_next_overwrites:
            self.fail_next_overwrites -= 1
            raise CommitFailedException("concurrent snapshot")
        self.rows = [row for row in self.rows if not _matches(row, overwrite_filter)]
        self.rows.extend(data.to_pylist())
        self.overwrites += 1
        self.snapshot_id = self.overwrites

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


class _Catalog:
    def __init__(self) -> None:
        self.tables: dict[tuple[str, str], _Table] = {}

    def create_namespace_if_not_exists(self, namespace: tuple[str]) -> None:
        assert namespace[0] in {"meta", "bronze"}

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
        "pipeline_run_id": "run-a", "orchestrator_run_id": None, "job_name": "bronze_parse",
        "code_git_sha": None, "image_digest": None, "config_hash": "config-1",
        "parameter_set_id": None, "started_at": datetime(2026, 9, 20, tzinfo=UTC), "finished_at": None,
        "published_at": None, "status": "running", "retry_count": 0,
        "input_row_count": None, "output_row_count": None, "quality_result_json": None,
        "metrics_json": None, "error_code": None,
    }
    store.upsert_meta_row(("meta", "pipeline_runs"), ("pipeline_run_id",), base)
    store.upsert_meta_row(("meta", "pipeline_runs"), ("pipeline_run_id",), {**base, "pipeline_run_id": "run-b"})
    store.upsert_meta_row(("meta", "pipeline_runs"), ("pipeline_run_id",), {**base, "status": "succeeded"})
    rows = catalog.tables[("meta", "pipeline_runs")].rows
    assert {row["pipeline_run_id"]: row["status"] for row in rows} == {
        "run-a": "succeeded", "run-b": "running",
    }
    assert store.get_meta_row(("meta", "pipeline_runs"), {"pipeline_run_id": "run-a"})["status"] == "succeeded"


def test_meta_upsert_refreshes_and_retries_optimistic_commit_conflict() -> None:
    catalog = _Catalog()
    store = IcebergTableStore(catalog)
    identifier = ("meta", "pipeline_runs")
    table = store.ensure_table(identifier)
    table.fail_next_overwrites = 1
    row = {
        "pipeline_run_id": "run-a", "orchestrator_run_id": None, "job_name": "bronze_parse",
        "code_git_sha": None, "image_digest": None, "config_hash": "config-1",
        "parameter_set_id": None, "started_at": datetime(2026, 9, 20, tzinfo=UTC), "finished_at": None,
        "published_at": None, "status": "running", "retry_count": 0,
        "input_row_count": None, "output_row_count": None, "quality_result_json": None,
        "metrics_json": None, "error_code": None,
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
        ("other", "42"), ("a", "1"), ("a", "2"),
    }

    def broken_batches():
        yield [_basin("a", feature_id="new")]
        raise ValueError("parser failed in later batch")

    with pytest.raises(ValueError, match="later batch"):
        store.replace_object_batches(table_id, "a", broken_batches())
    assert {(row["object_id"], row["source_feature_id"]) for row in table.rows} == {
        ("other", "42"), ("a", "1"), ("a", "2"),
    }


def test_batched_object_replacement_rejects_duplicate_keys_across_batches() -> None:
    store = IcebergTableStore(_Catalog())
    with pytest.raises(ValueError, match="duplicate"):
        store.replace_object_batches(
            ("bronze", "basin_polygon_raw"), "a",
            iter([[_basin("a", feature_id="1")], [_basin("a", feature_id="1")]]),
        )
