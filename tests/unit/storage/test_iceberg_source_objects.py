from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException

from flashflood_data.orchestration.landing.models import SourceObjectRow
from flashflood_data.storage.iceberg import (
    SourceObjectConflict,
    SourceObjectInventory,
    source_objects_arrow_schema,
)


def _row(object_id: str = "object-1") -> SourceObjectRow:
    now = datetime(2026, 9, 16, tzinfo=UTC)
    return SourceObjectRow(
        object_id=object_id,
        asset_id=f"asset-{object_id}",
        source_id="fixture-source",
        source_version="1",
        product="fixture",
        basin_level=12,
        object_uri=f"s3://raw/{object_id}.bin",
        manifest_uri=f"s3://raw/{object_id}/manifest.json",
        media_type="application/octet-stream",
        size_bytes=7,
        checksum="a" * 64,
        source_uri="https://example.invalid/source",
        retrieved_at=now,
        first_seen_at=now,
        ingest_run_id="run-1",
    )


class FakeScan:
    def __init__(self, existing: list[dict[str, object]]) -> None:
        self.existing = existing

    def to_arrow(self) -> pa.Table:
        fields = ["object_id", "checksum", "object_uri", "manifest_uri"]
        return pa.Table.from_pylist([{key: row[key] for key in fields} for row in self.existing])


class FakeTable:
    def __init__(self, existing: list[dict[str, object]], fail_commits: int = 0) -> None:
        self.existing = list(existing)
        self.fail_commits = fail_commits
        self.appended: pa.Table | None = None
        self.refresh_count = 0

    def scan(self, **_: object) -> FakeScan:
        return FakeScan(self.existing)

    def append(self, table: pa.Table) -> None:
        if self.fail_commits:
            self.fail_commits -= 1
            raise CommitFailedException("fixture conflict")
        self.appended = table
        self.existing.extend(table.to_pylist())

    def refresh(self) -> None:
        self.refresh_count += 1

    def current_snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(snapshot_id=42)


class FakeCatalog:
    def __init__(self, table: FakeTable) -> None:
        self.table = table
        self.namespace: tuple[str, ...] | None = None
        self.identifier: tuple[str, ...] | None = None
        self.schema: pa.Schema | None = None

    def create_namespace_if_not_exists(self, namespace: tuple[str, ...]) -> None:
        self.namespace = namespace

    def create_table_if_not_exists(
        self, identifier: tuple[str, ...], *, schema: pa.Schema, properties: dict[str, str]
    ) -> FakeTable:
        assert properties["format-version"] == "2"
        self.identifier = identifier
        self.schema = schema
        return self.table


def test_source_objects_schema_has_required_physical_types() -> None:
    schema = source_objects_arrow_schema()

    assert schema.field("object_id").type == pa.string()
    assert schema.field("object_id").nullable is False
    assert schema.field("object_uri").nullable is False
    assert schema.field("checksum").nullable is False
    assert schema.field("size_bytes").type == pa.int64()
    assert schema.field("basin_level").type == pa.int32()
    assert schema.field("basin_level").nullable is True
    assert schema.field("retrieved_at").type == pa.timestamp("us", tz="UTC")
    assert schema.field("provider_issued_at").nullable is True


def test_register_many_appends_one_batch_and_returns_snapshot() -> None:
    rows = [_row("object-1"), _row("object-2")]
    table = FakeTable(existing=[])
    inventory = SourceObjectInventory(FakeCatalog(table), ("meta", "source_objects"))

    result = inventory.register_many(rows)

    assert table.appended is not None
    assert table.appended.num_rows == len(rows)
    assert result.object_ids == tuple(row.object_id for row in rows)
    assert result.snapshot_id == 42
    assert result.reused == 0


def test_register_many_reuses_matching_identity_and_rejects_conflict() -> None:
    row = _row()
    matching = FakeTable(existing=[row.model_dump(mode="python")])
    result = SourceObjectInventory(FakeCatalog(matching)).register_many([row])
    assert result.reused == 1
    assert matching.appended is None

    conflicting_data = row.model_dump(mode="python") | {"checksum": "different"}
    conflicting = FakeTable(existing=[conflicting_data])
    with pytest.raises(SourceObjectConflict):
        SourceObjectInventory(FakeCatalog(conflicting)).register_many([row])


def test_register_many_refreshes_and_retries_optimistic_commit() -> None:
    table = FakeTable(existing=[], fail_commits=1)
    inventory = SourceObjectInventory(FakeCatalog(table))

    result = inventory.register_many([_row()])

    assert result.snapshot_id == 42
    assert table.refresh_count >= 2


def test_source_locations_returns_only_available_committed_payloads() -> None:
    available = _row("object-1").model_dump(mode="python")
    unavailable = _row("object-2").model_dump(mode="python") | {"status": "retired"}

    class LocationTable(FakeTable):
        def scan(self, **kwargs: object):
            assert kwargs["selected_fields"] == (
                "asset_id", "source_version", "checksum", "object_uri", "size_bytes", "status"
            )
            assert kwargs["row_filter"].term.name == "source_id"
            return SimpleNamespace(to_arrow=lambda: pa.Table.from_pylist([available, unavailable]))

    table = LocationTable(existing=[])
    inventory = SourceObjectInventory(FakeCatalog(table))

    assert inventory.source_locations("fixture-source") == {
        ("asset-object-1", "1", "a" * 64): ("s3://raw/object-1.bin", 7)
    }
    assert table.refresh_count == 1


def test_available_objects_filters_status_and_source_for_bronze_discovery() -> None:
    available = _row("object-1").model_dump(mode="python")
    unavailable = _row("object-2").model_dump(mode="python") | {"status": "retired"}

    class DiscoveryTable(FakeTable):
        def scan(self, **kwargs: object):
            assert "source_id" in str(kwargs["row_filter"])
            return SimpleNamespace(to_arrow=lambda: pa.Table.from_pylist([available, unavailable]))

    inventory = SourceObjectInventory(FakeCatalog(DiscoveryTable(existing=[])))
    assert [row.object_id for row in inventory.available_objects("fixture-source")] == ["object-1"]
