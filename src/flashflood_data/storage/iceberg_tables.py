"""Keyed Iceberg writes shared by raw-Meta and Bronze pipelines."""

import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo, Or

from flashflood_data.core.disk_index import DiskUniqueIndex
from flashflood_data.storage.iceberg_schemas import BRONZE_KEYS, table_schema


def _filter(key: Mapping[str, Any]) -> EqualTo | And:
    clauses = [EqualTo(name, value) for name, value in key.items()]
    if not clauses:
        raise ValueError("Iceberg key cannot be empty")
    result = clauses[0]
    for clause in clauses[1:]:
        result = And(result, clause)
    return result


def _filters(keys: Sequence[Mapping[str, Any]]) -> EqualTo | And | Or:
    if not keys:
        raise ValueError("Iceberg key batch cannot be empty")
    expressions = [_filter(key) for key in keys]
    result = expressions[0]
    for expression in expressions[1:]:
        result = Or(result, expression)
    return result


def _snapshot_id(table: Any) -> int:
    table.refresh()
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Iceberg write has no committed snapshot")
    return int(snapshot.snapshot_id)


def _canonical_rows(rows: Sequence[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    return sorted(
        ({name: value for name, value in row.items() if name != "ingest_run_id"} for row in rows),
        key=lambda row: tuple(str(row[name]) for name in keys),
    )


class IcebergTableStore:
    """Create contract tables and replace one logical key in one Iceberg transaction."""

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    def ensure_table(self, identifier: tuple[str, str]) -> Any:
        schema = table_schema(identifier)
        self.catalog.create_namespace_if_not_exists((identifier[0],))
        return self.catalog.create_table_if_not_exists(
            identifier,
            schema=schema,
            properties={"format-version": "2", "write.format.default": "parquet"},
        )

    def get_meta_row(
        self, identifier: tuple[str, str], key: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Return one logical Meta row, rejecting already duplicated keys."""
        if (identifier[0] != "meta" and not identifier[0].startswith("smoke_")) or identifier[1] == "source_objects":
            raise ValueError("get_meta_row requires a mutable Meta table")
        table = self.ensure_table(identifier)
        table.refresh()
        rows = table.scan(row_filter=_filter(key)).to_arrow().to_pylist()
        if len(rows) > 1:
            raise ValueError(f"duplicate existing Meta key in {identifier[1]}")
        return None if not rows else rows[0]

    def replace_object_rows(
        self,
        identifier: tuple[str, str],
        object_id: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """Replace one raw object's parsed slice; identical reruns keep the snapshot."""
        if (identifier[0] != "bronze" and not identifier[0].startswith("smoke_")) or identifier[1] not in BRONZE_KEYS:
            raise ValueError("object replacement requires a Bronze contract table")
        keys = BRONZE_KEYS[identifier[1]]
        requested = [dict(row) for row in rows]
        if any(row.get("object_id") != object_id for row in requested):
            raise ValueError("every Bronze row must match the requested object_id")
        record_keys = [tuple(row.get(name) for name in keys) for row in requested]
        if any(None in key for key in record_keys) or len(set(record_keys)) != len(record_keys):
            raise ValueError("duplicate or missing Bronze business key in object batch")
        schema = table_schema(identifier)
        payload = pa.Table.from_pylist(requested, schema=schema)
        table = self.ensure_table(identifier)
        expression = EqualTo("object_id", object_id)
        table.refresh()
        stored = table.scan(row_filter=expression).to_arrow().to_pylist()
        if _canonical_rows(stored, keys) == _canonical_rows(payload.to_pylist(), keys):
            return _snapshot_id(table)
        table.overwrite(payload, overwrite_filter=expression)
        return _snapshot_id(table)

    def replace_object_batches(
        self,
        identifier: tuple[str, str],
        object_id: str,
        batches: Iterable[Sequence[Mapping[str, Any]]],
    ) -> tuple[int, int]:
        """Atomically replace one object's rows without materializing all batches."""
        if (identifier[0] != "bronze" and not identifier[0].startswith("smoke_")) or identifier[1] not in BRONZE_KEYS:
            raise ValueError("object replacement requires a Bronze contract table")
        keys = BRONZE_KEYS[identifier[1]]
        schema = table_schema(identifier)
        table = self.ensure_table(identifier)
        table.refresh()
        count = 0
        with DiskUniqueIndex() as seen, table.transaction() as transaction:
            transaction.delete(EqualTo("object_id", object_id))
            for batch in batches:
                if not batch:
                    continue
                requested = [dict(row) for row in batch]
                if any(row.get("object_id") != object_id for row in requested):
                    raise ValueError("every Bronze row must match the requested object_id")
                identities = [tuple(row.get(name) for name in keys) for row in requested]
                if any(None in key for key in identities) or not seen.add_many([
                    json.dumps(key, default=str, separators=(",", ":")) for key in identities
                ]):
                    raise ValueError("duplicate or missing Bronze business key in object batch")
                transaction.append(pa.Table.from_pylist(requested, schema=schema))
                count += len(requested)
            if count == 0:
                raise ValueError("Bronze parse produced no rows")
        return _snapshot_id(table), count

    def upsert_meta_row(
        self,
        identifier: tuple[str, str],
        key_fields: tuple[str, ...],
        row: Mapping[str, Any],
    ) -> int:
        """Upsert one Meta row by its full logical key, leaving other keys intact."""
        return self.upsert_meta_rows(identifier, key_fields, (row,))

    def upsert_meta_rows(
        self,
        identifier: tuple[str, str],
        key_fields: tuple[str, ...],
        rows: Sequence[Mapping[str, Any]],
    ) -> int:
        """Upsert many Meta keys in one Iceberg commit."""
        if (identifier[0] != "meta" and not identifier[0].startswith("smoke_")) or identifier[1] == "source_objects":
            raise ValueError("Meta upsert cannot replace source_objects")
        requested = [dict(row) for row in rows]
        if not requested:
            raise ValueError("Meta upsert batch cannot be empty")
        keys = [{name: row[name] for name in key_fields} for row in requested]
        key_values = [tuple(key.values()) for key in keys]
        if len(set(key_values)) != len(key_values):
            raise ValueError(f"duplicate requested Meta key in {identifier[1]}")
        schema = table_schema(identifier)
        payload = pa.Table.from_pylist(requested, schema=schema)
        payload_rows = payload.to_pylist()
        expression = _filters(keys)
        table = self.ensure_table(identifier)
        last_error: CommitFailedException | None = None
        for _attempt in range(3):
            table.refresh()
            existing = table.scan(row_filter=expression).to_arrow().to_pylist()
            existing_keys = [tuple(row[name] for name in key_fields) for row in existing]
            if len(set(existing_keys)) != len(existing_keys):
                raise ValueError(f"duplicate existing Meta key in {identifier[1]}")
            existing_by_key = {
                tuple(row[name] for name in key_fields): row for row in existing
            }
            payload_by_key = {
                tuple(row[name] for name in key_fields): row for row in payload_rows
            }
            changed = any(
                key in existing_by_key and existing_by_key[key] != row
                for key, row in payload_by_key.items()
            )
            missing = [
                row for key, row in payload_by_key.items() if key not in existing_by_key
            ]
            if not changed and not missing:
                return _snapshot_id(table)
            try:
                if changed:
                    table.overwrite(payload, overwrite_filter=expression)
                else:
                    table.append(pa.Table.from_pylist(missing, schema=schema))
            except CommitFailedException as error:
                last_error = error
                continue
            return _snapshot_id(table)
        raise CommitFailedException(
            f"Meta upsert conflict after 3 attempts: {identifier[1]}"
        ) from last_error
