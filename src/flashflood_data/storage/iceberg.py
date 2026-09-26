"""Polaris-backed Iceberg inventory for immutable source objects."""

import json
from collections.abc import Sequence

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo, In

from flashflood_data.core.lakehouse import LakehouseSettings
from flashflood_data.orchestration.landing.models import RegisteredBatch, SourceObjectRow

DEFAULT_TABLE_IDENTIFIER = ("meta", "source_objects")
_IDENTITY_FIELDS = ("object_id", "checksum", "object_uri", "manifest_uri")


class SourceObjectConflict(RuntimeError):
    """Raised when one object identity points to different immutable content."""


class IcebergCommitError(RuntimeError):
    """Raised when bounded optimistic retries cannot commit the batch."""


def source_objects_arrow_schema() -> pa.Schema:
    """Return the stable physical schema of ``meta.source_objects``."""
    required_string = lambda name: pa.field(name, pa.string(), nullable=False)
    optional_string = lambda name: pa.field(name, pa.string(), nullable=True)
    utc = pa.timestamp("us", tz="UTC")
    return pa.schema(
        [
            required_string("object_id"),
            required_string("asset_id"),
            required_string("source_id"),
            required_string("source_version"),
            required_string("source_type"),
            required_string("product"),
            pa.field("basin_level", pa.int32(), nullable=True),
            required_string("object_uri"),
            required_string("manifest_uri"),
            required_string("media_type"),
            pa.field("size_bytes", pa.int64(), nullable=False),
            required_string("checksum_algorithm"),
            required_string("checksum"),
            required_string("source_uri"),
            optional_string("provider_issued_at"),
            optional_string("model_run_time"),
            optional_string("valid_time"),
            optional_string("available_at"),
            pa.field("retrieved_at", utc, nullable=False),
            pa.field("first_seen_at", utc, nullable=False),
            required_string("ingest_run_id"),
            required_string("status"),
            required_string("selection_json"),
            required_string("provider_metadata_json"),
        ]
    )


def load_polaris_catalog(settings: LakehouseSettings) -> Catalog:
    """Load the configured Polaris REST catalog without exposing credentials."""
    return load_catalog(
        settings.polaris_catalog,
        type="rest",
        uri=settings.polaris_uri,
        warehouse=settings.polaris_catalog,
        credential=(
            f"{settings.polaris_client_id.get_secret_value()}:"
            f"{settings.polaris_client_secret.get_secret_value()}"
        ),
        scope="PRINCIPAL_ROLE:ALL",
    )


class SourceObjectInventory:
    """Idempotently append verified source-object rows to one Iceberg table."""

    def __init__(
        self,
        catalog: Catalog,
        table_identifier: tuple[str, str] = DEFAULT_TABLE_IDENTIFIER,
    ) -> None:
        self.catalog = catalog
        self.table_identifier = table_identifier
        self.catalog.create_namespace_if_not_exists((table_identifier[0],))
        self.table = self.catalog.create_table_if_not_exists(
            table_identifier,
            schema=source_objects_arrow_schema(),
            properties={"format-version": "2", "write.format.default": "parquet"},
        )

    @staticmethod
    def _requested(rows: Sequence[SourceObjectRow]) -> dict[str, SourceObjectRow]:
        requested: dict[str, SourceObjectRow] = {}
        for row in rows:
            previous = requested.get(row.object_id)
            if previous is not None and any(
                getattr(previous, field) != getattr(row, field) for field in _IDENTITY_FIELDS
            ):
                raise SourceObjectConflict(f"conflicting batch identity: {row.object_id}")
            requested[row.object_id] = row
        return requested

    def _existing(self, object_ids: tuple[str, ...]) -> dict[str, dict[str, object]]:
        if not object_ids:
            return {}
        row_filter = (
            EqualTo("object_id", object_ids[0])
            if len(object_ids) == 1
            else In("object_id", object_ids)
        )
        table = self.table.scan(
            row_filter=row_filter,
            selected_fields=_IDENTITY_FIELDS,
        ).to_arrow()
        return {str(row["object_id"]): row for row in table.to_pylist()}

    def source_locations(self, source_id: str) -> dict[tuple[str, str, str], tuple[str, int]]:
        """Find committed MinIO payloads by local asset identity for recovery."""
        self.table.refresh()
        rows = self.table.scan(
            row_filter=EqualTo("source_id", source_id),
            selected_fields=(
                "asset_id", "source_version", "checksum", "object_uri", "size_bytes", "status"
            ),
        ).to_arrow().to_pylist()
        locations: dict[tuple[str, str, str], tuple[str, int]] = {}
        for row in rows:
            if row["status"] != "available":
                continue
            identity = (str(row["asset_id"]), str(row["source_version"]), str(row["checksum"]))
            locations.setdefault(identity, (str(row["object_uri"]), int(row["size_bytes"])))
        return locations

    def available_objects(
        self,
        source_id: str,
        *,
        object_ids: Sequence[str] | None = None,
    ) -> tuple[SourceObjectRow, ...]:
        """Discover verified raw objects eligible for source-specific Bronze parsing."""
        requested_ids = tuple(dict.fromkeys(object_ids or ()))
        if object_ids is not None and not requested_ids:
            return ()
        row_filter = And(
            EqualTo("source_id", source_id), EqualTo("status", "available")
        )
        if requested_ids:
            object_filter = (
                EqualTo("object_id", requested_ids[0])
                if len(requested_ids) == 1
                else In("object_id", requested_ids)
            )
            row_filter = And(row_filter, object_filter)
        self.table.refresh()
        rows = self.table.scan(row_filter=row_filter).to_arrow().to_pylist()
        return tuple(
            SourceObjectRow.model_validate(row)
            for row in rows if row["source_id"] == source_id and row["status"] == "available"
        )

    @staticmethod
    def _assert_matching(
        requested: dict[str, SourceObjectRow], existing: dict[str, dict[str, object]]
    ) -> None:
        for object_id, stored in existing.items():
            wanted = requested[object_id]
            if any(stored[field] != getattr(wanted, field) for field in _IDENTITY_FIELDS[1:]):
                raise SourceObjectConflict(f"conflicting Iceberg identity: {object_id}")

    def register_many(self, rows: Sequence[SourceObjectRow]) -> RegisteredBatch:
        """Append missing rows once and return the committed snapshot identity."""
        requested = self._requested(rows)
        if not requested:
            raise ValueError("source object batch cannot be empty")
        sources = {row.source_id for row in requested.values()}
        run_ids = {row.ingest_run_id for row in requested.values()}
        if len(sources) != 1 or len(run_ids) != 1:
            raise ValueError("one inventory batch must belong to one source and run")
        object_ids = tuple(sorted(requested))

        last_error: CommitFailedException | None = None
        reused = 0
        for _attempt in range(3):
            self.table.refresh()
            existing = self._existing(object_ids)
            self._assert_matching(requested, existing)
            reused = len(existing)
            missing = [requested[object_id] for object_id in object_ids if object_id not in existing]
            if missing:
                payload = [row.model_dump(mode="python") for row in missing]
                try:
                    self.table.append(
                        pa.Table.from_pylist(payload, schema=source_objects_arrow_schema())
                    )
                except CommitFailedException as exc:
                    last_error = exc
                    continue
            self.table.refresh()
            snapshot = self.table.current_snapshot()
            return RegisteredBatch(
                source_id=next(iter(sources)),
                run_id=next(iter(run_ids)),
                object_ids=object_ids,
                snapshot_id=None if snapshot is None else snapshot.snapshot_id,
                reused=reused,
            )
        raise IcebergCommitError(
            json.dumps({"code": "iceberg_commit_failed", "attempts": 3})
        ) from last_error
